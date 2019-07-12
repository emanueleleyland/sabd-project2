package kafka_streams;

import entity.Comment;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;


/**
 * Query2 implementation: returns the number of direct comments in every two-hours time slot. Results are emitted
 * every 24 hours, 7 days and 30 days
 */
public class Query2 {


	public static void doQuery(KStream<String, Comment> kStream) {

		KStream<Integer, Long> dailyStream = kStream
				//Filter all direct comment
				.filter((k, v) -> v.getCommentType().equals("comment"))
				.map((k, v) -> new KeyValue<>(1L, 1L))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
				//aggregate results every 2 hours
				.windowedBy(TimeWindows.of(Duration.ofHours(2)))
				//count the comments number in the time slot
				.reduce(Long::sum)
				.suppress(Suppressed.untilWindowCloses(unbounded()))
				.toStream()
				//emit a tuple with the time slot information
				.map((k, v) -> new KeyValue<>(Instant.ofEpochMilli(k.window().start()).atZone(ZoneId.of("UTC")).getHour(), v));

		dailyStream
				//group by time slot number (0...12)
				.groupByKey(Grouped.with(Serdes.Integer(), Serdes.Long()))
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				//aggregate values in the same time slot every day
				.reduce(Long::sum)
				.toStream()
				.map((k, v) -> new KeyValue<>(k.key(), v))
				.to("kafka_query2_1Day", Produced.with(Serdes.Integer(), Serdes.Long()));
/*
		dailyStream
				//group by time slot number (0...12)
				.groupByKey(Grouped.with(Serdes.Integer(), Serdes.Long()))
				.windowedBy(new WeeklyWindow())
				//aggregate values in the same time slot every 7 days
				.reduce(Long::sum)
				.toStream()
				.map((k, v) -> new KeyValue<>(k.key(), v))
				.to("kafka_query2_7Days", Produced.with(Serdes.Integer(), Serdes.Long()));

		dailyStream
				//group by time slot number (0...12)
				.groupByKey(Grouped.with(Serdes.Integer(), Serdes.Long()))
				.windowedBy(new MonthlyWindow())
				//aggregate values in the same time slot every 30 days
				.reduce(Long::sum)
				.toStream()
				.map((k, v) -> new KeyValue<>(k.key(), v))
				.to("kafka_query2_1Month", Produced.with(Serdes.Integer(), Serdes.Long()));
*/

	}


}
