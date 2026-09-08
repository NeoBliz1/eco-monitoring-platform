import java.time.Instant

def list = props.get("TIMESTAMPS")
if (list != null) {
    long currentTimestamp = Instant.now().toEpochMilli()
    list.add(currentTimestamp)

    log.info("Spatial Test: Registered 10-minute snapshot marker: " + currentTimestamp + ". Total pool: " + list.size())
}
