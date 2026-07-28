package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

import java.util.Map;

public interface TelemetryQueryRepository {

    Map<Object, Object> findRawGridDataByBucketFloor(String historyKey);
}
