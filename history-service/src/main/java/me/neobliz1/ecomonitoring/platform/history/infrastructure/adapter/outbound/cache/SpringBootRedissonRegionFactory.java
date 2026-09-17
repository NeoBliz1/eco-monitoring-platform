package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.cache;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.redisson.api.RedissonClient;
import org.redisson.hibernate.RedissonRegionFactory;

import java.util.Map;

public class SpringBootRedissonRegionFactory extends RedissonRegionFactory {

    private final RedissonClient redissonClient;

    public SpringBootRedissonRegionFactory(RedissonClient redissonClient) {
        super();
        this.redissonClient = redissonClient;
    }

    @Override
    protected RedissonClient createRedissonClient(StandardServiceRegistry registry, Map properties) {
        if(redissonClient==null) {
            throw new IllegalStateException(
                    "L2 Cache Bridge Initialization Failed: The shared Spring RedissonClient instance has not been bound yet."
            );
        }
        return redissonClient;
    }
}