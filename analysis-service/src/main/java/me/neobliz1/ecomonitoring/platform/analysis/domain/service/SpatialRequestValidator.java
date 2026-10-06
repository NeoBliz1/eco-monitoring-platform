package me.neobliz1.ecomonitoring.platform.analysis.domain.service;

import lombok.experimental.UtilityClass;
import me.neobliz1.ecomonitoring.platform.analysis.domain.model.dto.WeatherMapAnalysisRequestQuery;
import me.neobliz1.ecomonitoring.platform.model.exception.CoordinatesSquareTooLargeException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesBoundariesException;
import me.neobliz1.ecomonitoring.platform.model.exception.InvalidCoordinatesSquareException;

@UtilityClass
public class SpatialRequestValidator {

    public static void validateCoordinatesBox(WeatherMapAnalysisRequestQuery query) {
        Double minLat = query.minLat();
        Double maxLat = query.maxLat();
        Double minLon = query.minLon();
        Double maxLon = query.maxLon();
        if(minLat<-90.0 || maxLat>90.0 || minLon<-180.0 || maxLon>180.0)
            throw new InvalidCoordinatesBoundariesException();
        if(!(maxLat>minLat && maxLon>minLon)) throw new InvalidCoordinatesSquareException();
        if(Math.abs(maxLat-minLat)>5.0 || Math.abs(maxLon-minLon)>5.0) throw new CoordinatesSquareTooLargeException();
    }
}