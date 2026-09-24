package me.neobliz1.ecomonitoring.platform.analysis.infrastructure.adapter.outbound.messaging.kafka.record;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

public record WeatherMapRecord(@NonNull String key, @NonNull WeatherMap payload) {

    @Override
    public @NonNull String key() {
        return key;
    }

    @Override
    public @NonNull WeatherMap payload() {
        return payload;
    }
}