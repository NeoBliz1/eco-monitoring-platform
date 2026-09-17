package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.cache;

import com.github.benmanes.caffeine.cache.Cache;
import org.jspecify.annotations.NonNull;
import org.redisson.api.RTopic;
import org.springframework.cache.caffeine.CaffeineCache;

public class ClusterEvictingCaffeineCache extends CaffeineCache {

    public static final String CLEAR_ALL = "__CLEAR_ALL__";

    private final RTopic pubSubTopic;

    public ClusterEvictingCaffeineCache(String name, Cache<Object, Object> cache, RTopic pubSubTopic) {
        super(name, cache);
        this.pubSubTopic = pubSubTopic;
    }

    @Override
    public void put(@NonNull Object key, Object value) {
        super.put(key, value);
    }

    @Override
    public void evict(@NonNull Object key) {
        super.evict(key);
        evictExternalNodesCache(key);
    }

    public void evictExternalNodesCache(@NonNull Object key) {
        pubSubTopic.publish(key);
    }

    @Override
    public void clear() {
        super.clear();
        pubSubTopic.publish(CLEAR_ALL);
    }
}