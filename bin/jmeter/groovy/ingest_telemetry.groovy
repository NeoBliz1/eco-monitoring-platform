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

final double MIN_LAT = 40.5000d
final double MAX_LAT = 40.9000d
final double MIN_LON = -74.2500d
final double MAX_LON = -73.7000d

final String POST_METHOD = "POST"
final String CONTENT_TYPE_HEADER = "Content-Type"
final String PROTOBUF_MEDIA_TYPE = "application/x-protobuf"
final String BASE_INGESTION_URL = "http://localhost:8000/api/v1/telemetry/mono"

final def random = ThreadLocalRandom.current()

final double lat = MIN_LAT + (random.nextDouble() * (MAX_LAT - MIN_LAT))
final double lon = MIN_LON + (random.nextDouble() * (MAX_LON - MIN_LON))
final double alt = random.nextDouble() * 300.0

final String stationNum = String.format("%05d", random.nextInt(1, 10000))
final String compositeStationId = "000001" + stationNum
final long epochMilli = Instant.now().toEpochMilli()

def location = Location.newBuilder()
        .setLatitude(lat)
        .setLongitude(lon)
        .setAltitude(alt)
        .build()

List<SensorReading> readingsList = new ArrayList<>()

def ambient = AmbientReading.newBuilder()
        .setTemperatureC((float) (-99.0f + random.nextFloat() * 198.0f))
        .setHumidityPct((float) (random.nextFloat() * 100.0f))
        .setPressureHpa((float) (1.0f + random.nextFloat() * 1498.0f))
        .setLeafWetnessPct((float) (random.nextFloat() * 100.0f))
        .build()
readingsList.add(SensorReading.newBuilder().setAmbient(ambient).build())

if (random.nextBoolean()) {
    def wind = WindReading.newBuilder()
            .setSpeedMps((float) (random.nextFloat() * 75.0f))
            .setDirectionDeg(random.nextInt(361))
            .setGustMps((float) (random.nextFloat() * 120.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setWind(wind).build())
}

if (random.nextBoolean()) {
    def air = AirQualityReading.newBuilder()
            .setPm100((float) (random.nextFloat() * 400.0f))
            .setPm25((float) (random.nextFloat() * 250.0f))
            .setPm10((float) (random.nextFloat() * 300.0f))
            .setVocIndex((float) (random.nextFloat() * 500.0f))
            .setNoiseDb((float) (random.nextFloat() * 160.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setAirQuality(air).build())
}

if (random.nextBoolean()) {
    def precipitation = PrecipitationReading.newBuilder()
            .setRainRateMmH((float) (random.nextFloat() * 150.0f))
            .setSnowDepthCm((float) (random.nextFloat() * 300.0f))
            .setEvaporationRate((float) (random.nextFloat() * 25.0f))
            .build()
    readingsList.add(SensorReading.newBuilder().setPrecipitation(precipitation).build())
}

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
    connection.setDoOutput(true)

    connection.getOutputStream().write(protoBytes)
    int responseCode = connection.getResponseCode()

    SampleResult.setResponseCode(String.valueOf(responseCode))
    SampleResult.setResponseMessage(connection.getResponseMessage())
    SampleResult.setSuccessful(responseCode >= 200 && responseCode < 300)
    SampleResult.setSentBytes(protoBytes.length)

    if (responseCode >= 400) {
        connection.getErrorStream()?.readAllBytes()
    } else {
        connection.getInputStream()?.readAllBytes()
    }
} catch (Exception e) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseMessage("Ingestion Failure: " + e.getMessage())
} finally {
    if (connection != null) {
        connection.disconnect()
    }
}
