package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AnalysisUtilsTest {

    private static final long VALID_TIMESTAMP = 1700000000000L;
    private static final int ONE_MINUTE_INTERVAL = 60;
    private static final int FIVE_MINUTE_INTERVAL = 300;
    private static final int TEN_MINUTE_INTERVAL = 600;

    @Test
    void shouldReturnAlignedBucketFloor_whenTimestampIsExactMultiple() {
        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(VALID_TIMESTAMP, ONE_MINUTE_INTERVAL);

        assertEquals(1699999980000L, result);
    }

    @Test
    void shouldTruncateToIntervalFloor_whenTimestampHasRemainder() {
        long timestamp = 1700000059000L;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(timestamp, ONE_MINUTE_INTERVAL);

        assertEquals(1700000040000L, result);
    }

    @Test
    void shouldReturnCorrectBucketFloor_whenFiveMinuteIntervalIsSpecified() {
        long timestamp = 1700001234000L;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(timestamp, FIVE_MINUTE_INTERVAL);

        assertEquals(1700001000000L, result);
    }

    @Test
    void shouldReturnCorrectBucketFloor_whenTenMinuteIntervalIsSpecified() {
        long timestamp = 1700000599000L;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(timestamp, TEN_MINUTE_INTERVAL);

        assertEquals(1700000400000L, result);
    }

    @Test
    void shouldReturnSameTimestamp_whenTimestampIsExactlyAligned() {
        long alignedTimestamp = 1699999980000L;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(alignedTimestamp, ONE_MINUTE_INTERVAL);

        assertEquals(alignedTimestamp, result);
    }

    @Test
    void shouldReturnZeroBucketFloor_whenTimestampIsZero() {
        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(0L, ONE_MINUTE_INTERVAL);

        assertEquals(0L, result);
    }

    @Test
    void shouldReturnFloorLessThanInput_whenTimestampIsMaxValue() {
        long largeTimestamp = Long.MAX_VALUE;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(largeTimestamp, ONE_MINUTE_INTERVAL);

        assertTrue(result<largeTimestamp);
        assertEquals(0L, result%60000L);
    }

    @Test
    void shouldReturnTruncatedBucketFloor_whenTimestampIsJustBeforeNextInterval() {
        long timestamp = 1700000059999L;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(timestamp, ONE_MINUTE_INTERVAL);

        assertEquals(1700000040000L, result);
    }

    @Test
    void shouldReturnCorrectBucketFloor_whenIntervalIsSingleSecond() {
        long timestamp = 1700000005500L;

        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(timestamp, 1);

        assertEquals(1700000005000L, result);
    }

    @Test
    void shouldReturnSameTimestamp_whenTimestampIsLessThanOneInterval() {
        long expectedIntervalInMillis = 1L;
        long result = AnalysisUtils.getAggregationBucketFloorMillisInterval(expectedIntervalInMillis, ONE_MINUTE_INTERVAL);

        assertEquals(expectedIntervalInMillis, result);
    }

    @Test
    void shouldThrowIllegalArgumentException_whenTimestampIsNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> AnalysisUtils.getAggregationBucketFloorMillisInterval(-1L, ONE_MINUTE_INTERVAL));
    }
}