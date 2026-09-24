package me.neobliz1.ecomonitoring.platform.history.infrastructure.mapper;

import static me.neobliz1.ecomonitoring.platform.common.util.PlatformCommonUtils.getHeadersTextMapGetter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.RequiredArgsConstructor;
import me.neobliz1.ecomonitoring.platform.common.constant.PlatformConstants;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherGridCellMetric;
import me.neobliz1.ecomonitoring.platform.history.domain.model.entity.WeatherMapBucket;
import me.neobliz1.ecomonitoring.platform.history.domain.port.inbound.HistoricalDataConvertService;
import me.neobliz1.ecomonitoring.platform.history.infrastructure.adapter.outbound.persistence.postgres.jpa.HistoricalWeatherGridCellJpaRepository;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.GridCellLayers;
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.map.WeatherMap;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class WeatherMapConverter implements HistoricalDataConvertService {

    private final HistoricalWeatherGridCellJpaRepository gridCellJpaRepository;
    private final Tracer tracer = GlobalOpenTelemetry.getTracer("weather-map-converter");

    private static @NonNull WeatherGridCellMetric getWeatherGridCellMetric(@NonNull WeatherMapBucket bucket,
                                                                           @NonNull String geohashKey,
                                                                           @NonNull GridCellLayers layers) {
        WeatherGridCellMetric newCellMetric = new WeatherGridCellMetric(bucket, geohashKey);

        newCellMetric.setBucketId(bucket.getId());
        newCellMetric.setGeohash(geohashKey);
        newCellMetric.setReadingCount(layers.getReadingCount());

        newCellMetric.setAvgTemperature(layers.getAvgTemperature());
        newCellMetric.setAvgHumidity(layers.getAvgHumidity());
        newCellMetric.setAvgPressure(layers.getAvgPressure());
        newCellMetric.setAvgLeaf_wetnessPct(layers.getAvgLeafWetnessPct());

        newCellMetric.setAvgWindSpeed(layers.getAvgWindSpeed());
        newCellMetric.setAvgWindDirection(layers.getAvgWindDirection());

        newCellMetric.setAvgPm25(layers.getAvgPm25());
        newCellMetric.setAvgPm10(layers.getAvgPm10());
        newCellMetric.setAvgPm100(layers.getAvgPm100());

        newCellMetric.setAvgVoc(layers.getAvgVoc());
        newCellMetric.setAvgNoiseDb(layers.getAvgNoiseDb());

        newCellMetric.setAvgRainMm(layers.getAvgRainMm());
        newCellMetric.setAvgSnowCm(layers.getAvgSnowCm());
        newCellMetric.setAvgEvapRate(layers.getAvgEvapRate());

        newCellMetric.setAvgUvIndex(layers.getAvgUvIndex());
        newCellMetric.setAvgSolarRadiationWm2(layers.getAvgSolarRadiationWm2());
        newCellMetric.setAvgLux(layers.getAvgLux());
        newCellMetric.setAvgVisibilityM(layers.getAvgVisibilityM());
        return newCellMetric;
    }

    private static Double mergeWeightedAverage(Double oldVal, long oldCount, Double newVal, int newCount) {
        if(oldVal==null) return newVal;
        if(newVal==null) return oldVal;
        if(oldCount<=0) return newVal;
        if(newCount<=0) return oldVal;
        return ((oldVal*oldCount)+(newVal*newCount))/(double) (oldCount+newCount);
    }

    @Override
    public void mergeTelemetryInBatch(
            @NonNull WeatherMap weatherMap,
            @NonNull WeatherMapBucket bucket,
            @NonNull List<WeatherGridCellMetric> targetedCells) {
        if(weatherMap.getGridCellsMap().isEmpty()) {
            return;
        }
        Span mergeSpan = getMergeSpan(weatherMap, bucket, targetedCells);
        try(Scope ignored = mergeSpan.makeCurrent()) {
            Map<String, WeatherGridCellMetric> existingCellsMap = targetedCells.stream()
                    .collect(Collectors.toMap(
                            WeatherGridCellMetric::getGeohash,
                            cell -> cell,
                            (existing, replacement) -> existing
                    ));
            List<WeatherGridCellMetric> cellsToSave = new ArrayList<>();
            for(Map.Entry<String, GridCellLayers> entry : weatherMap.getGridCellsMap().entrySet()) {
                String geohashKey = entry.getKey();
                GridCellLayers layers = entry.getValue();
                if(layers==null) continue;
                WeatherGridCellMetric targetCell = existingCellsMap.get(geohashKey);
                if(targetCell!=null) {
                    mergeIntoExistingCell(targetCell, layers);
                } else {
                    WeatherGridCellMetric newCellMetric = getWeatherGridCellMetric(bucket, geohashKey, layers);
                    cellsToSave.add(newCellMetric);
                }
            }

            if(!cellsToSave.isEmpty()) {
                gridCellJpaRepository.saveAll(cellsToSave);
                mergeSpan.setAttribute("weather.cells.saved", cellsToSave.size());
            }

            mergeSpan.setStatus(StatusCode.OK);

        } catch(Exception e) {
            mergeSpan.recordException(e);
            mergeSpan.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            mergeSpan.end();
        }
    }

    @Override
    public @NonNull GridCellLayers convertWeatherGridCellsToWeatherMap(@NonNull WeatherGridCellMetric gridCellMetric) {
        return GridCellLayers.newBuilder()

                .setGeohash(gridCellMetric.getGeohash())
                .setReadingCount(gridCellMetric.getReadingCount())

                .setAvgTemperature(gridCellMetric.getAvgTemperature())
                .setAvgHumidity(gridCellMetric.getAvgHumidity())
                .setAvgPressure(gridCellMetric.getAvgPressure())
                .setAvgLeafWetnessPct(gridCellMetric.getAvgLeaf_wetnessPct())

                .setAvgWindSpeed(gridCellMetric.getAvgWindSpeed())
                .setAvgWindDirection(gridCellMetric.getAvgWindDirection())

                .setAvgPm25(gridCellMetric.getAvgPm25())
                .setAvgPm10(gridCellMetric.getAvgPm10())
                .setAvgPm100(gridCellMetric.getAvgPm100())

                .setAvgVoc(gridCellMetric.getAvgVoc())
                .setAvgNoiseDb(gridCellMetric.getAvgNoiseDb())

                .setAvgRainMm(gridCellMetric.getAvgRainMm())
                .setAvgSnowCm(gridCellMetric.getAvgSnowCm())
                .setAvgEvapRate(gridCellMetric.getAvgEvapRate())

                .setAvgUvIndex(gridCellMetric.getAvgUvIndex())
                .setAvgSolarRadiationWm2(gridCellMetric.getAvgSolarRadiationWm2())
                .setAvgLux(gridCellMetric.getAvgLux())
                .setAvgVisibilityM(gridCellMetric.getAvgVisibilityM())
                .build();
    }

    private void mergeIntoExistingCell(WeatherGridCellMetric targetCell, GridCellLayers layers) {
        int oldCount = targetCell.getReadingCount();
        int newCount = layers.getReadingCount();
        int combinedCount = oldCount+newCount;

        targetCell.setReadingCount(combinedCount);

        targetCell.setAvgTemperature(mergeWeightedAverage(targetCell.getAvgTemperature(), oldCount, layers.getAvgTemperature(), newCount));
        targetCell.setAvgHumidity(mergeWeightedAverage(targetCell.getAvgHumidity(), oldCount, layers.getAvgHumidity(), newCount));
        targetCell.setAvgPressure(mergeWeightedAverage(targetCell.getAvgPressure(), oldCount, layers.getAvgPressure(), newCount));
        targetCell.setAvgLeaf_wetnessPct(mergeWeightedAverage(targetCell.getAvgLeaf_wetnessPct(), oldCount, layers.getAvgLeafWetnessPct(), newCount));

        targetCell.setAvgWindSpeed(mergeWeightedAverage(targetCell.getAvgWindSpeed(), oldCount, layers.getAvgWindSpeed(), newCount));
        targetCell.setAvgWindDirection(mergeWeightedAverage(targetCell.getAvgWindDirection(), oldCount, layers.getAvgWindDirection(), newCount));

        targetCell.setAvgPm25(mergeWeightedAverage(targetCell.getAvgPm25(), oldCount, layers.getAvgPm25(), newCount));
        targetCell.setAvgPm10(mergeWeightedAverage(targetCell.getAvgPm10(), oldCount, layers.getAvgPm10(), newCount));
        targetCell.setAvgPm100(mergeWeightedAverage(targetCell.getAvgPm100(), oldCount, layers.getAvgPm100(), newCount));

        targetCell.setAvgVoc(mergeWeightedAverage(targetCell.getAvgVoc(), oldCount, layers.getAvgVoc(), newCount));
        targetCell.setAvgNoiseDb(mergeWeightedAverage(targetCell.getAvgNoiseDb(), oldCount, layers.getAvgNoiseDb(), newCount));

        targetCell.setAvgRainMm(mergeWeightedAverage(targetCell.getAvgRainMm(), oldCount, layers.getAvgRainMm(), newCount));
        targetCell.setAvgSnowCm(mergeWeightedAverage(targetCell.getAvgSnowCm(), oldCount, layers.getAvgSnowCm(), newCount));
        targetCell.setAvgEvapRate(mergeWeightedAverage(targetCell.getAvgEvapRate(), oldCount, layers.getAvgEvapRate(), newCount));

        targetCell.setAvgUvIndex(mergeWeightedAverage(targetCell.getAvgUvIndex(), oldCount, layers.getAvgUvIndex(), newCount));
        targetCell.setAvgSolarRadiationWm2(mergeWeightedAverage(targetCell.getAvgSolarRadiationWm2(), oldCount, layers.getAvgSolarRadiationWm2(), newCount));
        targetCell.setAvgLux(mergeWeightedAverage(targetCell.getAvgLux(), oldCount, layers.getAvgLux(), newCount));
        targetCell.setAvgVisibilityM(mergeWeightedAverage(targetCell.getAvgVisibilityM(), oldCount, layers.getAvgVisibilityM(), newCount));
    }

    private Span getMergeSpan(@NonNull WeatherMap weatherMap, @NonNull WeatherMapBucket bucket, @NonNull List<WeatherGridCellMetric> targetedCells) {
        Context parentContext = GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), weatherMap, getHeadersTextMapGetter());
        return tracer.spanBuilder(PlatformConstants.HISTORY_WEATHER_PACKET_TRACE_SPAN)
                .setParent(parentContext)
                .setAttribute("weather.bucket.id", bucket.getId().toString())
                .setAttribute("weather.bucket.timestamp", bucket.getTimestampBucket())
                .setAttribute("weather.grid.cells.count", weatherMap.getGridCellsMap().size())
                .setAttribute("weather.targeted.cells.count", targetedCells.size())
                .startSpan();
    }
}
