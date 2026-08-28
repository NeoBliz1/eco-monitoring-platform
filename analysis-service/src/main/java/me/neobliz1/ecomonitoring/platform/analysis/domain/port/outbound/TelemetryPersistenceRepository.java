package me.neobliz1.ecomonitoring.platform.analysis.domain.port.outbound;

public interface TelemetryPersistenceRepository {

    void saveRealTimeSlidingWindow(String geohashKey, String stationField, String timestampFormatted);

    void saveHistoricalGridCell(String geohash, byte[] serializedLayers);
}
