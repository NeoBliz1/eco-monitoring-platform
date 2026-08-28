package me.neobliz1.ecomonitoring.platform.analysis.domain.model;

import lombok.experimental.UtilityClass;

@UtilityClass
public class AnalysisConstants {

    public static final String ZERO_LOSS_ACCUMULATION_STORE = "zero-loss-accumulation-store";
    public static final String DEDUPLICATE_ROCKS_DB = "embedded-deduplicate-rocks-db";
    public static final String WEATHER_HOTWINDOW = "weather:hotwindow:";
    public static final String GRID_BUCKET_KEY_FORMAT = "%017d";
    public static final String HOT_WINDOW_PREFIX = "station:id:";
}