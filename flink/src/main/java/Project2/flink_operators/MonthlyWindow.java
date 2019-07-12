package Project2.flink_operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;

/**
 * Monthly window implementation.
 */
public class MonthlyWindow extends WindowAssigner<Object, TimeWindow> {

	public MonthlyWindow() {}

	@Override
	public Collection<TimeWindow> assignWindows(Object o, long timestamp, WindowAssignerContext windowAssignerContext) {
		if (timestamp > -9223372036854775808L) {
			LocalDateTime localDateTime = Instant.ofEpochMilli(timestamp).atZone(ZoneId.of("UTC")).toLocalDateTime();
			LocalDateTime start = localDateTime.withDayOfMonth(1).withHour(0).withMinute(0).withSecond(0).withNano(0);
			LocalDateTime end = localDateTime.withDayOfMonth(1).plusMonths(1).withHour(0).withMinute(0).withSecond(0).withNano(0).minusNanos(1);
			long startMillis = start.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
			long endMillis = end.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
			return Collections.singletonList(new TimeWindow(startMillis, endMillis));
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). Is the time characteristic set to 'ProcessingTime', or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
		return EventTimeTrigger.create();
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new Serializer();
	}

	@Override
	public boolean isEventTime() {
		return true;
	}
}
