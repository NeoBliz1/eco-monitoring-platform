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

final double MIN_LAT = 35.000d
final double MAX_LAT = 45.000d
final double MIN_LON = -104.000d
final double MAX_LON = -91.250d

final String BASE_INGESTION_URL = "http://localhost:8000/api/v1/telemetry/mono"
final String TX_ID_CONFIRMATION_URL = "http://localhost:8000/api/v1/tx-ingestion-history/weather-packet-tx-id"

final def random = ThreadLocalRandom.current()

final double lat = MIN_LAT + (random.nextDouble() * (MAX_LAT - MIN_LAT))
final double lon = MIN_LON + (random.nextDouble() * (MAX_LON - MIN_LON))

final double alt = random.nextDouble() * 300.0
final int cityNum = random.nextInt(1, 2001)

final String cityId = String.format("%06d", cityNum)
final String stationNum = String.format("%05d", random.nextInt(1, 10001))

final String compositeStationId = cityId + stationNum

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

def postRequest(String url, byte[] body, String contentType) {
    HttpURLConnection conn = null
    try {
        def uri = new URI(url).toURL()
        conn = (HttpURLConnection) uri.openConnection()
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Content-Type", contentType)
        conn.setConnectTimeout(5000)
        conn.setReadTimeout(5000)
        conn.setDoOutput(true)

        conn.getOutputStream().write(body)
        int responseCode = conn.getResponseCode()
        if (responseCode >= 400) {
            conn.getErrorStream()?.readAllBytes()
        } else {
            conn.getInputStream()?.readAllBytes()
        }
        return responseCode
    } catch (Exception e) {
        log.error("Request failed: " + e.getMessage())
        return 500
    } finally {
        conn?.disconnect()
    }
}

def ingestionTask = CompletableFuture.supplyAsync {
    postRequest(BASE_INGESTION_URL, protoBytes, "application/x-protobuf")
}

def txId = "sample-tx-id"
def confirmationTask = CompletableFuture.supplyAsync {
    postRequest(TX_ID_CONFIRMATION_URL, txId.getBytes("UTF-8"), "text/plain")
}

try {
    CompletableFuture.allOf(ingestionTask, confirmationTask).get(10, TimeUnit.SECONDS)

    int codeIngestion = ingestionTask.get()
    int codeConfirmation = confirmationTask.get()

    boolean isSuccess = (codeIngestion >= 200 && codeIngestion < 300) && (codeConfirmation >= 200 && codeConfirmation < 300)

    SampleResult.setResponseCode(String.valueOf(codeIngestion))
    SampleResult.setResponseMessage("Ingestion Status: " + codeIngestion + " | Confirmation Status: " + codeConfirmation)
    SampleResult.setSuccessful(isSuccess)
    SampleResult.setSentBytes(protoBytes.length + txId.getBytes("UTF-8").length)

} catch (Exception e) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseMessage("Parallel Request Synchronization Failure: " + e.getMessage())
}