package me.neobliz1.ecomonitoring.platform.history.domain.model.constant;

import lombok.experimental.UtilityClass;

@UtilityClass
public class HistoricalCacheConstants {

    public static final String BUCKETS_REGION = "weather_map_buckets_region";
    public static final String BUCKET_METRICS_REGION = "weather_bucket_cells_region";
    public static final String METRICS_REGION = "weather_grid_cell_metrics_region";
    public static final String QUERIES_REGION = "grpc_spatial_box_queries_region";
}