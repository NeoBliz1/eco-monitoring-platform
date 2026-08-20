import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AirQualityReading
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.AmbientReading
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.SensorReading
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WeatherPacket
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.WindReading
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.PrecipitationReading
import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.OpticalReading
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom

final String POST_METHOD = "POST"
final String CONTENT_TYPE_HEADER = "Content-Type"
final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf"
final String BASE_INGESTION_URL = "http://localhost:8000/api/v1/telemetry/mono"

final Map<String, Map<String, ? extends Object>> CITY_REGISTRY = [
        "NYC": [placeId: 1, lat: 40.7128d, lon: -74.0060d],
        "LA" : [placeId: 4, lat: 34.0522d, lon: -118.2437d],
        "CHI": [placeId: 3, lat: 41.8781d, lon: -87.6298d],
        "MIA": [placeId: 2, lat: 25.7617d, lon: -80.1918d]
]
final def CITY_KEYS = ["NYC", "LA", "CHI", "MIA"]

final def random = ThreadLocalRandom.current()

// --- 1. Dynamic 6+5 Composite Station ID Generation ---
final String targetedCityKey = CITY_KEYS[random.nextInt(CITY_KEYS.size())]
final Map<String, ? extends Object> selectedCityMetadata = CITY_REGISTRY.get(targetedCityKey)

final int placeId = (int) selectedCityMetadata.get("placeId")
final int stationNum = random.nextInt(1, 251)

// Generates exactly 11 numeric digits matching schema pattern: "^[0-9]{11}$" (e.g. Los Angeles Station #1 -> "00000400001")
final String compositeStationId = String.format("%06d%05d", placeId, stationNum)
final String logicalTrackingHeaderValue = "${targetedCityKey}-STATION-${String.format('%03d', stationNum)}"

final long epochMilli = Instant.now().toEpochMilli()

// Jitter coordinates localized within the actual boundary radius of the picked city
final double lat = selectedCityMetadata.lat + (random.nextDouble() - 0.5) * 1.5
final double lon = selectedCityMetadata.lon + (random.nextDouble() - 0.5) * 1.5
final double alt = random.nextDouble() * 500.0

final float temp = (float) (-99.0f + random.nextFloat() * 198.0f)    // Dynamic range checking [-100.0, 100.0]
final float humidity = (float) (random.nextFloat() * 100.0f)         // Dynamic range checking [0.0, 100.0]
final float pressure = (float) (1.0f + random.nextFloat() * 1498.0f) // Dynamic range checking [0.0, 1500.0]
final float leafWetness = (float) (random.nextFloat() * 100.0f)      // Dynamic range checking [0.0, 100.0]

def location = Location.newBuilder()
        .setLatitude(lat)
        .setLongitude(lon)
        .setAltitude(alt)
        .build()

List<SensorReading> readingsList = new ArrayList<>()

// Group 1: Ambient Data (Always added to guarantee schema requirement: repeated.min_items = 1)
def ambient = AmbientReading.newBuilder()
        .setTemperatureC(temp)
        .setHumidityPct(humidity)
        .setPressureHpa(pressure)
        .setLeafWetnessPct(leafWetness)
        .build()
readingsList.add(SensorReading.newBuilder().setAmbient(ambient).build())

// Group 2: Wind Data Stress Matrix
if (random.nextBoolean()) {
    def wind = WindReading.newBuilder()
            .setSpeedMps((float) (random.nextFloat() * 75.0f))
            .setDirectionDeg(random.nextInt(361)) // Stresses full rotation limits [0, 360]
            .setGustMps((float) (random.nextFloat() * 120.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setWind(wind).build())
}

// Group 3: Air Quality Data Stress Matrix
if (random.nextBoolean()) {
    def air = AirQualityReading.newBuilder()
            .setPm100((float) (random.nextFloat() * 400.0f))
            .setPm25((float) (random.nextFloat() * 250.0f)) // Validated camelCase method target
            .setPm10((float) (random.nextFloat() * 300.0f))
            .setVocIndex((float) (random.nextFloat() * 500.0f))
            .setNoiseDb((float) (random.nextFloat() * 160.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setAirQuality(air).build())
}

// Group 4: Precipitation Data Stress Matrix
if (random.nextBoolean()) {
    def precipitation = PrecipitationReading.newBuilder()
            .setRainRateMmH((float) (random.nextFloat() * 150.0f))
            .setSnowDepthCm((float) (random.nextFloat() * 300.0f))
            .setEvaporationRate((float) (random.nextFloat() * 25.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setPrecipitation(precipitation).build())
}

// Group 5: Optical Data Stress Matrix
if (random.nextBoolean()) {
    def optical = OpticalReading.newBuilder()
            .setUvIndex((float) (random.nextFloat() * 20.0f))
            .setSolarRadiationWm2((float) (random.nextFloat() * 1500.0f))
            .setLux((float) (random.nextFloat() * 120000.0f))
            .setVisibilityM((float) (random.nextFloat() * 10000.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setOptical(optical).build())
}

def packetBuilder = WeatherPacket.newBuilder()
        .setStationId(compositeStationId)
        .setTimestamp(epochMilli)
        .setLocation(location)

readingsList.each { reading -> packetBuilder.addReadings(reading) }
byte[] protoBytes = packetBuilder.build().toByteArray()

HttpURLConnection connection = null
try {
    def url = new URI(BASE_INGESTION_URL).toURL()
    connection = (HttpURLConnection) url.openConnection()
    connection.setRequestMethod(POST_METHOD)
    connection.setRequestProperty(CONTENT_TYPE_HEADER, PROTOBUF_MEDIA_TYPE)

    connection.setRequestProperty("X-Logical-Station-Id", logicalTrackingHeaderValue)
    connection.setDoOutput(true)

    connection.getOutputStream().write(protoBytes)
    int responseCode = connection.getResponseCode()

    SampleResult.setResponseCode(String.valueOf(responseCode))
    SampleResult.setResponseMessage(connection.getResponseMessage())
    SampleResult.setSuccessful(responseCode >= 200 && responseCode < 300)
    SampleResult.setSentBytes(protoBytes.length)

    SampleResult.setRequestHeaders(connection.getRequestProperties().entrySet()
            .collect { entry -> "${entry.key}: ${entry.value.join(', ')}" }.join('\n'))

    if (responseCode >= 400) {
        connection.getErrorStream()?.readAllBytes()
    } else {
        connection.getInputStream()?.readAllBytes()
    }
} catch (Exception e) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseMessage("Stress Script Exception: " + e.getMessage())
} finally {
    if (connection != null) {
        connection.disconnect()
    }
}
