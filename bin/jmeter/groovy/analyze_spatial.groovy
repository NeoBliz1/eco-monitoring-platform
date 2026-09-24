import java.util.Properties
import java.util.concurrent.ThreadLocalRandom

Properties props = binding.hasVariable('props') ? binding.getVariable('props') : new Properties()
def SampleResult = binding.hasVariable('SampleResult') ? binding.getVariable('SampleResult') : null

final String BASE_ANALYSIS_URL = "http://localhost:8000/api/v1/weather-map/spatial"

final double MIN_LAT = 35.000d
final double MAX_LAT = 45.000d
final double MIN_LON = -104.000d
final double MAX_LON = -91.250d

final double QUERY_BOX_SPAN = 0.5000d
final def random = ThreadLocalRandom.current()

double roundToThree(double val) {
    return Math.round(val * 1000.0d) / 1000.0d
}

def timestampList = props.get("TIMESTAMPS")
if (timestampList == null || timestampList.isEmpty()) {
    if (SampleResult != null) {
        SampleResult.setSuccessful(false)
        SampleResult.setResponseMessage("Analysis Failure: Waiting for timeline synchronization context initialization.")
    }
    return
}

int targetedIndex = random.nextInt(timestampList.size())
long pickedTimestamp = timestampList.get(targetedIndex)

def globalPool = props.get("EXISTING_LOCATIONS")

double minLat = 0.0d
double minLon = 0.0d
double maxLat = 0.0d
double maxLon = 0.0d
boolean hasMatch = false
int maxAttempts = 15
int attempts = 0

// If ingestion points are available, look for a matching boundary envelope
if (globalPool != null && !globalPool.isEmpty()) {
    while (!hasMatch && attempts < maxAttempts) {
        attempts++

        double rawMinLat = MIN_LAT + (random.nextDouble() * ((MAX_LAT - QUERY_BOX_SPAN) - MIN_LAT))
        double rawMinLon = MIN_LON + (random.nextDouble() * ((MAX_LON - QUERY_BOX_SPAN) - MIN_LON))

        minLat = roundToThree(rawMinLat)
        minLon = roundToThree(rawMinLon)
        maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
        maxLon = roundToThree(minLon + QUERY_BOX_SPAN)

        // Confirm box isolates at least one registered coordinate tracking artifact
        hasMatch = globalPool.any { point ->
            return (point.lat >= minLat && point.lat <= maxLat &&
                    point.lon >= minLon && point.lon <= maxLon)
        }
    }
}

// Fallback / Hard Override Strategy: If no coordinate validation can be found, force center the query
// =========================================================================
// 4. Phase B: Hardened Override Strategy (UPDATED TO PRESERVE MATCH)
// =========================================================================
if (!hasMatch) {
    if (globalPool != null && !globalPool.isEmpty()) {
        // Grab a guaranteed existing coordinate record
        def luckyPoint = globalPool.get(random.nextInt(globalPool.size()))

        // Pad the box safely around the point (point will be exactly in the center)
        double centeredMinLat = luckyPoint.lat - (QUERY_BOX_SPAN / 2.0d)
        double centeredMinLon = luckyPoint.lon - (QUERY_BOX_SPAN / 2.0d)

        // Bounding clamp checks to preserve absolute geographical ranges
        if (centeredMinLat < MIN_LAT) centeredMinLat = MIN_LAT
        if (centeredMinLat + QUERY_BOX_SPAN > MAX_LAT) centeredMinLat = MAX_LAT - QUERY_BOX_SPAN
        if (centeredMinLon < MIN_LON) centeredMinLon = MIN_LON
        if (centeredMinLon + QUERY_BOX_SPAN > MAX_LON) centeredMinLon = MAX_LON - QUERY_BOX_SPAN

        minLat = roundToThree(centeredMinLat)
        minLon = roundToThree(centeredMinLon)
        maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
        maxLon = roundToThree(minLon + QUERY_BOX_SPAN)
    } else {
        // Extreme Fallback: No items ingested yet, generate pure random bounds within safe margins
        double rawMinLat = MIN_LAT + (random.nextDouble() * ((MAX_LAT - QUERY_BOX_SPAN) - MIN_LAT))
        double rawMinLon = MIN_LON + (random.nextDouble() * ((MAX_LON - QUERY_BOX_SPAN) - MIN_LON))
        minLat = roundToThree(rawMinLat)
        minLon = roundToThree(rawMinLon)
        maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
        maxLon = roundToThree(minLon + QUERY_BOX_SPAN)
    }
}

// 5. Phase C: Execute Target API Query Pipeline Over Network Sockets
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

    if (SampleResult != null) {
        SampleResult.setResponseCode(String.valueOf(responseCode))
        SampleResult.setResponseMessage(connection.getResponseMessage())
        SampleResult.setSuccessful(responseCode >= 200 && responseCode < 300)
    }

    // Read streams to clear network socket buffers cleanly
    if (responseCode >= 400) {
        connection.getErrorStream()?.readAllBytes()
    } else {
        connection.getInputStream()?.readAllBytes()
    }
} catch (Exception e) {
    if (SampleResult != null) {
        SampleResult.setSuccessful(false)
        SampleResult.setResponseMessage("Analysis Query Exception: " + e.getMessage())
    }
} finally {
    if (connection != null) {
        connection.disconnect()
    }
}