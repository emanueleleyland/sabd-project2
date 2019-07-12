package Project2.metrics;


import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

public class NanoTimestamp {
    public static Long getNanoTimestamp(){
        return Duration.between(Instant.ofEpochMilli(0), Instant.now(NanoClock.system(ZoneId.of("UTC")))).toNanos();
    }
}
