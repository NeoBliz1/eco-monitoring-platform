import java.util.concurrent.CopyOnWriteArrayList
import java.time.Instant

if (props.get("TIMESTAMPS") == null) {
    def list = new CopyOnWriteArrayList<Long>()
    list.add(Instant.now().toEpochMilli())
    props.put("TIMESTAMPS", list)
}
