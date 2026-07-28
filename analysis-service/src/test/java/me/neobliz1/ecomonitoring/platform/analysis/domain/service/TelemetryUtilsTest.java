package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class TelemetryUtilsTest {

    private static final long VALID_TIMESTAMP = 1700000000000L;
    private static final int ONE_MINUTE_INTERVAL = 60;
    private static final int FIVE_MINUTE_INTERVAL = 300;
    private static final int TEN_MINUTE_INTERVAL = 600;

    @Test
    void shouldReturnAlignedBucketFloor_whenTimestampIsExactMultiple() {
        long result = TelemetryUtils.getAggregationBucketFloorInterval(VALID_TIMESTAMP, ONE_MINUTE_INTERVAL);

        assertThat(result).isEqualTo(1699999980000L);
    }

    @Test
    void shouldTruncateToIntervalFloor_whenTimestampHasRemainder() {
        long timestamp = 1700000059000L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(timestamp, ONE_MINUTE_INTERVAL);

        assertThat(result).isEqualTo(1700000040000L);
    }

    @Test
    void shouldReturnCorrectBucketFloor_whenFiveMinuteIntervalIsSpecified() {
        long timestamp = 1700001234000L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(timestamp, FIVE_MINUTE_INTERVAL);

        assertThat(result).isEqualTo(1700001000000L);
    }

    @Test
    void shouldReturnCorrectBucketFloor_whenTenMinuteIntervalIsSpecified() {
        long timestamp = 1700000599000L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(timestamp, TEN_MINUTE_INTERVAL);

        assertThat(result).isEqualTo(1700000400000L);
    }

    @Test
    void shouldReturnSameTimestamp_whenTimestampIsExactlyAligned() {
        long alignedTimestamp = 1699999980000L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(alignedTimestamp, ONE_MINUTE_INTERVAL);

        assertThat(result).isEqualTo(alignedTimestamp);
    }

    @Test
    void shouldReturnZeroBucketFloor_whenTimestampIsZero() {
        long result = TelemetryUtils.getAggregationBucketFloorInterval(0L, ONE_MINUTE_INTERVAL);

        assertThat(result).isZero();
    }

    @Test
    void shouldReturnZeroBucketFloor_whenTimestampIsOneMillisecond() {
        long result = TelemetryUtils.getAggregationBucketFloorInterval(1L, ONE_MINUTE_INTERVAL);

        assertThat(result).isZero();
    }

    @Test
    void shouldReturnFloorLessThanInput_whenTimestampIsMaxValue() {
        long largeTimestamp = Long.MAX_VALUE;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(largeTimestamp, ONE_MINUTE_INTERVAL);

        assertThat(result).isLessThan(largeTimestamp);
        assertThat(result%60000L).isZero();
    }

    @Test
    void shouldReturnTruncatedBucketFloor_whenTimestampIsJustBeforeNextInterval() {
        long timestamp = 1700000059999L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(timestamp, ONE_MINUTE_INTERVAL);

        assertThat(result).isEqualTo(1700000040000L);
    }

    @Test
    void shouldReturnCorrectBucketFloor_whenIntervalIsSingleSecond() {
        long timestamp = 1700000005500L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(timestamp, 1);

        assertThat(result).isEqualTo(1700000005000L);
    }

    @Test
    void shouldThrowArithmeticException_whenAggregationIntervalIsZero() {
        assertThatThrownBy(() -> TelemetryUtils.getAggregationBucketFloorInterval(VALID_TIMESTAMP, 0))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void shouldReturnZeroBucketFloor_whenTimestampIsNegativeAndSmall() {
        long timestamp = -1L;

        long result = TelemetryUtils.getAggregationBucketFloorInterval(timestamp, ONE_MINUTE_INTERVAL);

        assertThat(result).isZero();
    }
}
