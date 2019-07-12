package kafka_streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Comment;
import entity.RankRecord;
import kafka_operators.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import utils.SerializerAny;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
/**
 * Query 1 implementation: Returns the ranking of the three most popular articles
 * i.e. those who received the highest number of comments (direct or indirect).
 * Results are emitted every hour, day and week (of event time)
 */
public class Query1 {


	public static void doQuery(KStream<String, Comment> kStream) {

		//Emit a tuple composed only by the article ID
		KStream<Windowed<String>, Long> stream_1h = kStream
				.map((k, v) -> new KeyValue<>(v.getArticleID(), 1L))
				//key by article id
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
				//Process the value over a hourly Tumbling window
				.windowedBy(TimeWindows.of(Duration.ofHours(1)))
				//Count the numbers of comment referred to the given article
				.reduce(Long::sum)
				.suppress(Suppressed.untilWindowCloses(unbounded()))
				.toStream();


		stream_1h
				.map(new Query1PartialRankMapper())
				//key by a pseudo random number between 1 and # of task managers
				//to compute a partial distributed ranking
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(TimeWindows.of(Duration.ofHours(1)))
				//compute partial rank
				.aggregate(new Query1RankInitializer(), new Query1PartialRankAggregator())
				.toStream()
				.map((k, v) -> new KeyValue<Long, byte[]>(1L, v))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(TimeWindows.of(Duration.ofHours(1)))
				//emit the global rank
				.aggregate(new Query1RankInitializer(),
						new Query1GlobalRankAggregator())
				.toStream()
				//write result as string and serialize it on kafka
				.map(new Query1RankResultMapper())
				.to("kafka_query1_1Hour", Produced.with(Serdes.Long(), Serdes.String()));


		KStream<Windowed<String>, Long> stream_1d = stream_1h
				.map((stringWindowed, aLong) -> new KeyValue<>(stringWindowed.key(), aLong))
				//key by article id
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
				//Process the value over a daily Tumbling window
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				//Count the numbers of comment referred to the given article
				.reduce(Long::sum)
				.suppress(Suppressed.untilWindowCloses(unbounded()))
				.toStream();

		stream_1d.map(new Query1PartialRankMapper())
				//key by a pseudo random number between 1 and # of task managers
				//to compute a partial distributed ranking
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				//compute partial rank
				.aggregate(new Query1RankInitializer(), new Query1PartialRankAggregator())
				.toStream()
				.map((k, v) -> new KeyValue<Long, byte[]>(1L, v))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				//emit the global rank
				.aggregate(new Query1RankInitializer(),
						new Query1GlobalRankAggregator())
				.toStream()
				.map(new Query1RankResultMapper())
				.to("kafka_query1_1Day", Produced.with(Serdes.Long(), Serdes.String()));


		KStream<Windowed<String>, Long> stream_7 = stream_1d
				.map((stringWindowed, aLong) -> new KeyValue<>(stringWindowed.key(), aLong))
				//key by article id
				.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
				//Process the value over a weekly Tumbling window
				.windowedBy(TimeWindows.of(Duration.ofDays(7)))
				//Count the numbers of comment referred to the given article
				.reduce(Long::sum)
				.suppress(Suppressed.untilWindowCloses(unbounded()))
				.toStream();

		stream_7.map(new Query1PartialRankMapper())

				//key by a pseudo random number between 1 and # of task managers
				//to compute a partial distributed ranking
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(TimeWindows.of(Duration.ofDays(7)))
				//Compute partial rank
				.aggregate(new Query1RankInitializer(), new Query1PartialRankAggregator())
				.toStream()
				.map((k, v) -> new KeyValue<Long, byte[]>(1L, v))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(TimeWindows.of(Duration.ofDays(7)))
				//emit the global rank
				.aggregate(new Query1RankInitializer(),
						new Query1GlobalRankAggregator())
				.toStream()
				.map(new Query1RankResultMapper())
				//write results on kafka
				.to("kafka_query1_7Days", Produced.with(Serdes.Long(), Serdes.String()));

	}
}
