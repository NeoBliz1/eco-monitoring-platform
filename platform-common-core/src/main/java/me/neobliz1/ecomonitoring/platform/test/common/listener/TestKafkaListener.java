package me.neobliz1.ecomonitoring.platform.test.common.listener;

import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestKafkaListener<T> implements AutoCloseable {
    private final Consumer<String, T> consumer;
    @Getter
    private final List<T> receivedPackets = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public TestKafkaListener(Consumer<String, T> consumer) {
        this.consumer = consumer;
        this.executor.submit(this::pollLoop);
    }

    private void pollLoop() {
        try {
            while(running.get()) {
                ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, T> record : records) {
                    receivedPackets.add(record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    @Override
    public void close() {
        running.set(false);
        executor.shutdownNow();
    }
}