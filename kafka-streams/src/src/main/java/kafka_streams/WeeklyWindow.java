package kafka_streams;

import org.apache.kafka.streams.internals.ApiUtils;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class WeeklyWindow extends Windows<TimeWindow> {

	private final long maintainDurationMs = 86400000L;
	public final long sizeMs = Duration.ofDays(7).toMillis();
	public final long advanceMs = Duration.ofDays(7).toMillis();
	private final long graceMs = -1L;
	private final long offset = Duration.ofDays(-3).toMillis();

	public Map<Long, TimeWindow> windowsFor(long timestamp) {
		long windowStart = Math.max(0L, timestamp - this.sizeMs + this.advanceMs) / this.advanceMs * this.advanceMs + this.offset;

		LinkedHashMap<Long, TimeWindow> windows;
		for (windows = new LinkedHashMap<>(); windowStart <= timestamp; windowStart += this.advanceMs) {
			TimeWindow window = new TimeWindow(windowStart, windowStart + this.sizeMs);
			windows.put(windowStart, window);
		}

		return windows;
	}

	public long size() {
		return this.sizeMs;
	}

	public long gracePeriodMs() {
		return this.graceMs != -1L ? this.graceMs : this.maintainMs() - this.size();
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o != null && this.getClass() == o.getClass()) {
			org.apache.kafka.streams.kstream.TimeWindows that = (org.apache.kafka.streams.kstream.TimeWindows) o;
			return this.sizeMs == that.sizeMs && this.advanceMs == that.advanceMs;
		} else {
			return false;
		}
	}

	public int hashCode() {
		return Objects.hash(new Object[]{this.maintainDurationMs, this.segments, this.sizeMs, this.advanceMs, this.graceMs});
	}

	public String toString() {
		return "TimeWindows{maintainDurationMs=" + this.maintainDurationMs + ", sizeMs=" + this.sizeMs + ", advanceMs=" + this.advanceMs + ", graceMs=" + this.graceMs + '}';
	}


}
