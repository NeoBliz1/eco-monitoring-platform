import java.util.concurrent.ThreadLocalRandom

final String BASE_ANALYSIS_URL = "http://localhost:8000/api/v1/weather-map/spatial"

final double MASTER_MIN_LAT = 40.5000d
final double MASTER_MAX_LAT = 40.9000d
final double MASTER_MIN_LON = -74.2500d
final double MASTER_MAX_LON = -73.7000d

static double roundToThree(double val) {
    return Math.round(val * 1000.0d) / 1000.0d
}

final double QUERY_BOX_SPAN = 0.0010d

final def random = ThreadLocalRandom.current()

def timestampList = props.get("TIMESTAMPS")
if (timestampList == null || timestampList.isEmpty()) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseMessage("Analysis Failure: Waiting for timeline synchronization context initialization.")
    return
}

int targetedIndex = random.nextInt(timestampList.size())
long pickedTimestamp = timestampList.get(targetedIndex)

double rawMinLat = MASTER_MIN_LAT + (random.nextDouble() * ((MASTER_MAX_LAT - QUERY_BOX_SPAN) - MASTER_MIN_LAT))
double rawMinLon = MASTER_MIN_LON + (random.nextDouble() * ((MASTER_MAX_LON - QUERY_BOX_SPAN) - MASTER_MIN_LON))

double minLat = roundToThree(rawMinLat)
double minLon = roundToThree(rawMinLon)
double maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
double maxLon = roundToThree(minLon + QUERY_BOX_SPAN)

String queryString = String.format(
        "?targetTimestamp=%d&min-lat=%f&max-lat=%f&min-lon=%f&max-lon=%f",
        pickedTimestamp, minLat, maxLat, minLon, maxLon
)

HttpURLConnection connection = null
try {
    def url = new URI(BASE_ANALYSIS_URL + queryString).toURL()
    connection = (HttpURLConnection) url.openConnection()
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Accept", "application/json")

    int responseCode = connection.getResponseCode()
    SampleResult.setResponseCode(String.valueOf(responseCode))
    SampleResult.setResponseMessage(connection.getResponseMessage())
    SampleResult.setSuccessful(responseCode >= 200 && responseCode < 300)

    if (responseCode >= 400) {
        connection.getErrorStream()?.readAllBytes()
    } else {
        connection.getInputStream()?.readAllBytes()
    }
} catch (Exception e) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseMessage("Analysis Query Exception: " + e.getMessage())
} finally {
    if (connection != null) {
        connection.disconnect()
    }
}