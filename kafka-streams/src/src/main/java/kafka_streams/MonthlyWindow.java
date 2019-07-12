package kafka_streams;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windows;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class MonthlyWindow extends Windows<TimeWindow> {

	public long sizeMs = 0;
	public long advanceMs = 0;
	private final long graceMs = -1L;
	private final long maintainDurationMs = 86400000L;

	@Override
	public Map<Long, TimeWindow> windowsFor(long timestamp) {
		LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC")).toLocalDateTime();
		LocalDateTime start = localDateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
		LocalDateTime end = localDateTime.withDayOfMonth(1).plusMonths(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
		long windowStart = start.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
		long endMillis = end.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
		this.sizeMs = endMillis - windowStart;
		this.advanceMs = this.sizeMs;
		LinkedHashMap<Long, TimeWindow> windows;
		for (windows = new LinkedHashMap<>(); windowStart <= timestamp; windowStart += this.advanceMs) {
			TimeWindow window = new TimeWindow(windowStart, windowStart + this.sizeMs);
			windows.put(windowStart, window);
		}
		return windows;
	}

	@Override
	public long size() {
		return this.sizeMs;
	}

	@Override
	public long gracePeriodMs() {
		return this.graceMs != -1L ? this.graceMs : Math.max(this.maintainDurationMs, this.sizeMs) - this.size();
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o != null && this.getClass() == o.getClass()) {
			TimeWindows that = (TimeWindows) o;
			return this.sizeMs == that.sizeMs && this.advanceMs == that.advanceMs;
		} else {
			return false;
		}
	}

	public int hashCode() {
		return Objects.hash(new Object[]{this.maintainDurationMs, this.sizeMs, this.advanceMs, this.graceMs});
	}

	public String toString() {
		return "TimeWindows{maintainDurationMs=" + this.maintainDurationMs + ", sizeMs=" + this.sizeMs + ", advanceMs=" + this.advanceMs + ", graceMs=" + this.graceMs + '}';
	}
}
