package me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres;

import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherMapJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@RequiredArgsConstructor
public class WeatherMapBucketCreationService {

    private final HistoricalWeatherMapJpaRepository jpaRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW, isolation = Isolation.READ_COMMITTED)
    public WeatherMapBucket saveWeatherMapBucket(WeatherMap weatherMap) {
        WeatherMapBucket bucket = new WeatherMapBucket();
        bucket.setId(HistoricalPersistenceRepositoryAdapter.getBucketId(weatherMap));
        bucket.setTimestampBucket(weatherMap.getTimestampBucket());
        bucket.setIntervalMinutes(weatherMap.getIntervalMinutes());
        return jpaRepository.saveAndFlush(bucket);
    }
}