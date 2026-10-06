import me.neobliz1.ecomonitoring.platform.shared.contracts.proto.Location
import java.net.URLEncoder
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ThreadLocalRandom

final def safeLog = log

Properties props = binding.hasVariable('props') ? binding.getVariable('props') : new Properties()

// ----- Spatial bounds (must match ingest_telemetry.groovy) -----
final double MIN_LAT = 35.000d
final double MAX_LAT = 45.000d
final double MIN_LON = -104.000d
final double MAX_LON = -91.250d

// ----- Analysis / query config -----
final Duration THREE_HOURS = Duration.ofHours(3)
final double QUERY_BOX_SPAN = 0.5d          // degrees (~55 km N-S); tune to taste
final int    MAX_MATCH_ATTEMPTS = 10        // rejection-sampling budget for Phase A
final String ANALYSIS_URL = "http://localhost:8000/api/v1/weather-map/spatial"

final def random = ThreadLocalRandom.current()

// ------------------------------------------------------------------
// Helper: round to 3 decimals (matches ingestion's rounding)
// ------------------------------------------------------------------
def roundToThree = { double v -> Math.round(v * 1000.0d) / 1000.0d }

// ------------------------------------------------------------------
// Helper: read lat/lon from a pool entry regardless of shape
//   - ingest script stores LinkedHashMap: [lat: .., lon: ..]
//   - a.txt snippet uses point.lat / point.lon (Expando/POGO style)
// ------------------------------------------------------------------
def entryLat = { entry ->
    if (entry instanceof Map) return ((Number) entry.get("lat")).doubleValue()
    return ((Number) entry.getProperty("lat")).doubleValue()
}
def entryLon = { entry ->
    if (entry instanceof Map) return ((Number) entry.get("lon")).doubleValue()
    return ((Number) entry.getProperty("lon")).doubleValue()
}

// ------------------------------------------------------------------
// 1. Resolve shared test start anchor (JVM-global via props)
// ------------------------------------------------------------------
Instant startInstant
synchronized (props) {
    Object existing = props.get("ANALYSIS_START_TIME")
    if (existing == null) {
        startInstant = Instant.now()
        props.put("ANALYSIS_START_TIME", startInstant.toEpochMilli())
    } else {
        long millis = (existing instanceof Long) ? (Long) existing : Long.parseLong(existing.toString())
        startInstant = Instant.ofEpochMilli(millis)
    }
}

// ------------------------------------------------------------------
// 2. Random timestamp window
//    - now > start + 3h -> (now - 3h, now)
//    - else             -> (start, now)
// ------------------------------------------------------------------
final Instant now = Instant.now()
final Instant from
final Instant to
if (now.isAfter(startInstant.plus(THREE_HOURS))) {
    from = now.minus(THREE_HOURS)
    to   = now
} else {
    from = startInstant
    to   = now
}

Instant chosen
long windowMillis = Duration.between(from, to).toMillis()
if (windowMillis <= 0L) {
    chosen = from
} else {
    chosen = from.plusMillis(random.nextLong(windowMillis))
}
final long epochMilli = chosen.toEpochMilli()
final long fromMillis = from.toEpochMilli()
final long toMillis   = to.toEpochMilli()

// ------------------------------------------------------------------
// 3. Snapshot the global location pool (written by ingest script)
// ------------------------------------------------------------------
CopyOnWriteArrayList globalPool = null
Object poolObj = props.get("EXISTING_LOCATIONS")
if (poolObj instanceof CopyOnWriteArrayList) {
    globalPool = (CopyOnWriteArrayList) poolObj
} else if (poolObj instanceof List) {
    globalPool = new CopyOnWriteArrayList((List) poolObj)
}
final int poolSize = (globalPool == null) ? 0 : globalPool.size()

// ------------------------------------------------------------------
// 4. Phase A: rejection-sample a random box that isolates >=1 pool point
// ------------------------------------------------------------------
boolean hasMatch = false
int attempts = 0

double minLat = 0d, minLon = 0d, maxLat = 0d, maxLon = 0d

if (globalPool != null && !globalPool.isEmpty()) {
    while (!hasMatch && attempts < MAX_MATCH_ATTEMPTS) {
        attempts++

        double rawMinLat = MIN_LAT + (random.nextDouble() * ((MAX_LAT - QUERY_BOX_SPAN) - MIN_LAT))
        double rawMinLon = MIN_LON + (random.nextDouble() * ((MAX_LON - QUERY_BOX_SPAN) - MIN_LON))

        minLat = roundToThree(rawMinLat)
        minLon = roundToThree(rawMinLon)
        maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
        maxLon = roundToThree(minLon + QUERY_BOX_SPAN)

        // Confirm box isolates at least one registered coordinate
        final double fMinLat = minLat, fMaxLat = maxLat, fMinLon = minLon, fMaxLon = maxLon
        hasMatch = globalPool.any { point ->
            double pLat = entryLat(point)
            double pLon = entryLon(point)
            return (pLat >= fMinLat && pLat <= fMaxLat &&
                    pLon >= fMinLon && pLon <= fMaxLon)
        }
    }
}

// ------------------------------------------------------------------
// 5. Phase B: Hardened Override Strategy (preserve match)
// ------------------------------------------------------------------
if (!hasMatch) {
    if (globalPool != null && !globalPool.isEmpty()) {
        // Grab a guaranteed existing coordinate record
        def luckyPoint = globalPool.get(random.nextInt(globalPool.size()))
        double pLat = entryLat(luckyPoint)
        double pLon = entryLon(luckyPoint)

        // Center the box on the chosen point
        double centeredMinLat = pLat - (QUERY_BOX_SPAN / 2.0d)
        double centeredMinLon = pLon - (QUERY_BOX_SPAN / 2.0d)

        // Clamp within absolute bounding envelope
        if (centeredMinLat < MIN_LAT) centeredMinLat = MIN_LAT
        if (centeredMinLat + QUERY_BOX_SPAN > MAX_LAT) centeredMinLat = MAX_LAT - QUERY_BOX_SPAN
        if (centeredMinLon < MIN_LON) centeredMinLon = MIN_LON
        if (centeredMinLon + QUERY_BOX_SPAN > MAX_LON) centeredMinLon = MAX_LON - QUERY_BOX_SPAN

        minLat = roundToThree(centeredMinLat)
        minLon = roundToThree(centeredMinLon)
        maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
        maxLon = roundToThree(minLon + QUERY_BOX_SPAN)

        hasMatch = true
    } else {
        // Extreme fallback: no ingested points yet, generate pure random bounds
        double rawMinLat = MIN_LAT + (random.nextDouble() * ((MAX_LAT - QUERY_BOX_SPAN) - MIN_LAT))
        double rawMinLon = MIN_LON + (random.nextDouble() * ((MAX_LON - QUERY_BOX_SPAN) - MIN_LON))
        minLat = roundToThree(rawMinLat)
        minLon = roundToThree(rawMinLon)
        maxLat = roundToThree(minLat + QUERY_BOX_SPAN)
        maxLon = roundToThree(minLon + QUERY_BOX_SPAN)
    }
}

// ------------------------------------------------------------------
// 6. Center point of the box (used as the query anchor location)
// ------------------------------------------------------------------
double centerLat = roundToThree((minLat + maxLat) / 2.0d)
double centerLon = roundToThree((minLon + maxLon) / 2.0d)
double centerAlt = random.nextDouble() * 300.0

def location = Location.newBuilder()
        .setLatitude(centerLat)
        .setLongitude(centerLon)
        .setAltitude(centerAlt)
        .build()

// ------------------------------------------------------------------
// 7. HTTP helper
// ------------------------------------------------------------------
def doGet(String url, def safeLog) {
    HttpURLConnection conn = null
    InputStream is = null
    InputStream es = null
    byte[] body = null
    try {
        def uri = new URI(url).toURL()
        conn = (HttpURLConnection) uri.openConnection()
        conn.setRequestMethod("GET")
        conn.setRequestProperty("Accept", "application/x-protobuf, application/json")
        conn.setConnectTimeout(2000)
        conn.setReadTimeout(5000)

        int responseCode = conn.getResponseCode()
        if (responseCode >= 400) {
            es = conn.getErrorStream()
            body = es?.readAllBytes() ?: new byte[0]
        } else {
            is = conn.getInputStream()
            body = is?.readAllBytes() ?: new byte[0]
        }
        return [code: responseCode, bytes: body]
    } catch (Exception e) {
        safeLog.error("Analysis request failed on URL [ " + url + " ]: " + e.getMessage())
        return [code: 500, bytes: new byte[0]]
    } finally {
        is?.close()
        es?.close()
        conn?.disconnect()
    }
}

// ------------------------------------------------------------------
// 8. Build query URL and execute
// ------------------------------------------------------------------
String queryString = String.format(
        "?target-timestamp=%d&min-lat=%f&max-lat=%f&min-lon=%f&max-lon=%f",
        epochMilli, minLat, maxLat, minLon, maxLon
)

String queryUrl = ANALYSIS_URL + queryString

try {
    def result = doGet(queryUrl, safeLog)
    int code = (int) result.code
    byte[] respBytes = (byte[]) result.bytes

    boolean ok = (code >= 200 && code < 300)

    SampleResult.setResponseCode(String.valueOf(code))
    SampleResult.setResponseMessage(
            "Analysis: " + code +
                    " | box=[" + minLat + "," + minLon + " -> " + maxLat + "," + maxLon + "]" +
                    " | ts=" + epochMilli +
                    " | window=[" + fromMillis + "," + toMillis + ")" +
                    " | pool=" + poolSize +
                    " | attempts=" + attempts +
                    " | matched=" + hasMatch +
                    " | bytes=" + respBytes.length
    )
    SampleResult.setSuccessful(ok)
    SampleResult.setSentBytes(0)
    SampleResult.setBytes(respBytes.length)
} catch (Exception e) {
    SampleResult.setSuccessful(false)
    SampleResult.setResponseMessage("Execution Failure: " + e.getMessage())
}