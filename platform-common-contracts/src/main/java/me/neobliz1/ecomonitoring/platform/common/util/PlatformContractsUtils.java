package me.neobliz1.ecomonitoring.platform.common.util;

import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket;
import org.jspecify.annotations.NonNull;

public class PlatformContractsUtils {

    public static @NonNull String getUniqueTxId(@NonNull WeatherPacket packet) {
        return packet.getStationId()+":"+packet.getTimestamp();
    }
}