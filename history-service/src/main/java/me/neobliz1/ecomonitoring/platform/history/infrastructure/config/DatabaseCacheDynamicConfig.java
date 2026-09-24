package me.neobliz1.ecomonitoring.platform.history.infrastructure.config;

import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKETS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.BUCKET_METRICS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.METRICS_REGION;
import static me.neobliz1.ecomonitoring.platform.history.domain.model.constant.HistoricalCacheConstants.QUERIES_REGION;
import static me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.cache.ClusterEvictingCaffeineCache.CLEAR_ALL;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.RequiredArgsConstructor;
import lombok.val;
import me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.cache.ClusterEvictingCaffeineCache;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.cache.SpringBootRedissonRegionFactory;
import me.neobliz1.ecomonitoring.platform.model.record.ServiceAddressRecord;
import org.hibernate.cfg.AvailableSettings;
import org.jspecify.annotations.NonNull;
import org.redisson.Redisson;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.support.SimpleCacheManager;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Configuration
@RequiredArgsConstructor
public class DatabaseCacheDynamicConfig {

    private final ConfigurableEnvironment environment;
    private final HistoryInfrastructureProperties infraProps;

    private static void putRegion(Map<String, Object> props, String region,
                                  String maxEntries, String maxIdle, String ttl) {
        if(maxEntries!=null) {
            props.put(String.format("hibernate.cache.redisson.%s.eviction.max_entries", region), maxEntries);
        }
        if(maxIdle!=null) {
            props.put(String.format("hibernate.cache.redisson.%s.expiration.max_idle_time", region), maxIdle);
        }
        if(ttl!=null) {
            props.put(String.format("hibernate.cache.redisson.%s.expiration.time_to_live", region), ttl);
        }
    }

    @Bean
    public HibernatePropertiesCustomizer bindRedissonL2CacheToHibernate(RedissonClient redissonClient) {
        val region = infraProps.getData().getHibernate().getCache().getLevel2().getRegion();
        val regionWmb = region.getWeatherMapBucket();
        val regionWgc = region.getWeatherGridCells();
        val regionSqr = region.getSpatialQueryResults();
        String regionWmbL2CacheMaxSize = String.valueOf(regionWmb.getL2CacheMaxSize());
        int regionWmbL2CacheTtlAfterAccessMinutes = regionWmb.getL2CacheTtlAfterAccessMinutes();
        return hibernateProperties -> {
            hibernateProperties.put(
                    AvailableSettings.CACHE_REGION_FACTORY,
                    new SpringBootRedissonRegionFactory(redissonClient)
            );
            String regionWmbTtlAfterAccessInMillis = String.valueOf(Duration.ofMinutes(regionWmbL2CacheTtlAfterAccessMinutes).toMillis());
            putRegion(hibernateProperties, BUCKETS_REGION, regionWmbL2CacheMaxSize, regionWmbTtlAfterAccessInMillis, null);
            putRegion(hibernateProperties, BUCKET_METRICS_REGION, regionWmbL2CacheMaxSize, regionWmbTtlAfterAccessInMillis, null);
            putRegion(hibernateProperties, METRICS_REGION, String.valueOf(regionWgc.getL2CacheMaxSize()),
                    String.valueOf(Duration.ofMinutes(regionWgc.getL2CacheTtlAfterAccessMinutes()).toMillis()), null);
            putRegion(hibernateProperties, QUERIES_REGION, String.valueOf(regionSqr.getL2CacheMaxSize()), null,
                    String.valueOf(Duration.ofMinutes(regionSqr.getL2CacheTtlAfterWriteMinutes()).toMillis()));
            hibernateProperties.put(
                    "hibernate.cache.redisson.default-update-timestamps-region.expiration.time_to_live", "0");
        };
    }

    @Bean
    public CacheManager springL1CacheManager(RedissonClient redissonClient) {
        SimpleCacheManager cacheManager = new SimpleCacheManager();
        val level1 = infraProps.getData().getHibernate().getCache().getLevel1();
        int l1CacheMaxSize = level1.getL1CacheMaxSize();
        String invalidationTopic = level1.getInvalidationTopicName();
        int l1CacheTtlMinutes = level1.getL1CacheTtlMinutes();
        CaffeineCache bucketCache = createClusterAwareL1Cache(BUCKETS_REGION, redissonClient, invalidationTopic, l1CacheTtlMinutes,
                l1CacheMaxSize);
        CaffeineCache queryCache = createClusterAwareL1Cache(QUERIES_REGION, redissonClient, invalidationTopic, l1CacheTtlMinutes,
                l1CacheMaxSize);
        cacheManager.setCaches(List.of(bucketCache, queryCache));
        return cacheManager;
    }

    @Bean(destroyMethod = "shutdown")
    public RedissonClient dynamicRedissonClient(DiscoveryClient discoveryClient) {
        val redis = infraProps.getData().getRedis();
        ServiceAddressRecord serviceAddress = PlatformCommonUtils.discoverServiceAddressFromConsulServerByName(
                discoveryClient, environment, redis.getServiceName());

        Config config = new Config();
        String redisUrl = String.format("redis://%s:%d", serviceAddress.resolvedHost(), serviceAddress.resolvedPort());
        config.useSingleServer()
                .setAddress(redisUrl)
                .setConnectionPoolSize(64)
                .setConnectionMinimumIdleSize(24);
        String redisPassword = redis.getPassword();
        if(redisPassword!=null && !redisPassword.isBlank()) {
            config.setPassword(redisPassword);
        }
        return Redisson.create(config);
    }

    private @NonNull CaffeineCache createClusterAwareL1Cache(String cacheName, @NonNull RedissonClient redissonClient,
                                                             String invalidationTopicPrefix, int l1CacheTtlMinutes, int l1CacheMaxSize) {
        Cache<Object, Object> nativeCaffeineCache = Caffeine.newBuilder()
                .maximumSize(l1CacheMaxSize)
                .expireAfterAccess(l1CacheTtlMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build();
        RTopic topic = redissonClient.getTopic(invalidationTopicPrefix+cacheName);
        topic.addListener(Object.class, (channel, expiredKey) -> {
            if(CLEAR_ALL.equals(expiredKey)) {
                nativeCaffeineCache.invalidateAll();
            } else {
                nativeCaffeineCache.invalidate(expiredKey);
            }
        });
        return new ClusterEvictingCaffeineCache(cacheName, nativeCaffeineCache, topic);
    }
}