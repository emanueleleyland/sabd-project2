package kafka_streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.*;
import kafka_operators.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import redis.clients.jedis.Jedis;
import utils.SerializerAny;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;


/**
 * Query3 implementation: returns the 10 most popular users. The popularity score is based on the
 * likes received for each comment and the number of replies received by other users.
 * Results are emitted every day, week and every month
 */

public class Query3 {

	private static Double wa = 0.3, wb = 0.7, approvedInc = 1.1;
	private static String redisRoot = "KS_";

	public static void doQuery(KStream<String, Comment> kStream) {

		//Create new Jedis connection
		Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
		ObjectMapper mapper = new ObjectMapper();

		//Returns a DataStream of direct comments (depth = 1)
		//with fields: commentId, recommendation, userId
		KStream<Windowed<Long>, byte[]> comments_1 = kStream
				//filter depth = 1
				.filter((k, v) -> Long.valueOf(v.getDepth()).equals(1L))
				//Increment recommendation if the comments is selected by the editor
				//and store the tuple on Redis DB
				.map(new Query3Depth1Mapper(approvedInc, jedis, redisRoot, mapper));

		//OUTPUT: Tuple3 commentId #likes userId

		//Returns a DataStream of indirect comments (depth = 2)
		//with: commentId userId inReplyTo
		KStream<Windowed<Long>, byte[]> comments_2 = kStream
				.filter((k, v) -> Long.valueOf(v.getDepth()).equals(2L))
				//store the tuple on Redis DB
				.map(new Query3Depth2Mapper(approvedInc, jedis, redisRoot, mapper));

		//OUTPUT comment2: Tuple3 commentId userId inReplyTo

		//Return a DataStream of indirect comments with fields:
		//inreplyTo commentType
		KStream<Windowed<Long>, byte[]> indirectStream = kStream
				.filter((k, v) -> v.getCommentType().equals("userReply"))
				.map((k, v) -> new KeyValue<>(Long.valueOf(v.getInReplyTo()), 1L))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				//Compute the number of indirect comments referred to each comment
				//Emit a datastream with fields: inReplyTo, #indirectComments
				.reduce(Long::sum)
				.suppress(Suppressed.untilWindowCloses(unbounded()))
				.toStream()
				.map((k, v) -> new KeyValue<>(k, SerializerAny.serialize(v)));

		//OUTPUT: Tuple2: inReplyTo(commentId) #indirectComments

		doQueryTime(comments_1, comments_2,  indirectStream, jedis, mapper, 1L, "kafka_query3_1Day");
		doQueryTime(comments_1, comments_2,  indirectStream, jedis, mapper, 7L, "kafka_query3_7Days");
		doQueryTime(comments_1, comments_2,  indirectStream, jedis, mapper, 30L, "kafka_query3_30Days");
		//---------------------------------------------------------------------------------------------------------------------------------------------------------------

	}

	public static void doQueryTime(KStream<Windowed<Long>, byte[]> comments_1, KStream<Windowed<Long>, byte[]> comments_2, KStream<Windowed<Long>, byte[]> indirectStream, Jedis jedis, ObjectMapper mapper, Long days, String topic) {

		//Join indirectComments Datastream with depth-2 comments
		//Returns a datastream with fields:
		//commentid #IndirectComments #likes userid inReplyTo
		KStream<Long, byte[]> comments_2_indirect =
				indirectStream
						.merge(comments_2)
						.map(new Query3Depth2And3MergeMapper())
						.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
						.windowedBy(days == 1L ? TimeWindows.of(Duration.ofDays(1)) : days == 7L ? new WeeklyWindow() : new MonthlyWindow())
						.aggregate(new Query3DepthsMergeInitializer(),
								new Query3Depth2And3MergeAggregator()).toStream()
						.map(new Query3Depth2And3FinalMapper(jedis, mapper));

		//commentid indirectCount #likes userid inReplyTo*/

		//Join indirectComments Datastream with depth-1 comments
		//Returns a datastream with fields:
		//commentid #IndirectComments #likes userid
		KStream<Long, byte[]> comments_1_indirect = indirectStream
				.merge(comments_1)
				.map(new Query3Depth1And3MergeMapper())
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(days == 1L ? TimeWindows.of(Duration.ofDays(1)) : days == 7L ? new WeeklyWindow() : new MonthlyWindow())
				.aggregate(new Query3DepthsMergeInitializer(),
						new Query3Depth1And3MergeAggregator())
				.toStream()
				.map(new Query3Depth1And3FinalMapper(jedis, mapper));


		//commentId #likes indirectCount userID

		//Aggregate values of indirect count referred to a user
		KStream<Long, byte[]> comment_1_final = comments_1_indirect
				.merge(comments_2_indirect)
				.map(new Query3Depth1And2MergeMapper())
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(days == 1L ? TimeWindows.of(Duration.ofDays(1)) : days == 7L ? new WeeklyWindow() : new MonthlyWindow())
				.aggregate(new Query3DepthsMergeInitializer(),
						new Query3Depth1And2MergeAggregator())
				.toStream()
				.map((k, v) -> new KeyValue<>(k.key(), v));

		//Join Comment1 final stream with Comment2 final stream to compute for each user his score aggregating the value
		//of recommendation and indirect comments
		KStream<Long, byte[]> aggregateStream = comment_1_final
				.merge(comments_2_indirect)
				.map(new Query3FinalMergeMapper())
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(days == 1L ? TimeWindows.of(Duration.ofDays(1)) : days == 7L ? new WeeklyWindow() : new MonthlyWindow())
				.aggregate(new Query3FinalMergeInitializer(),
						new Query3FinalMergeAggregator())
				.toStream()
				.map((k, v) -> new KeyValue<>(k.key(), v));;


		aggregateStream
				//compute score for each user
				.map(new Query3PartialRankMapper())
				//Split the aggregated datastream for each taskmanager and emit a partial rank with at most 10 elements every day
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(days == 1L ? TimeWindows.of(Duration.ofDays(1)) : days == 7L ? new WeeklyWindow() : new MonthlyWindow())
				.aggregate(new Query3RankInitializer(),
						new Query3PartialRankAggregator())
				.toStream()
				.map((k, v) -> new KeyValue<>(1L, v))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.ByteArray()))
				.windowedBy(days == 1L ? TimeWindows.of(Duration.ofDays(1)) : days == 7L ? new WeeklyWindow() : new MonthlyWindow())
				//Compute total ranking sending all the partial ranks to one worker node
				.aggregate(new Query3RankInitializer(),
						new Query3GlobalRankAggregator())
				.toStream()
				.map(new Query3FinalRankMapper())
				.to(topic, Produced.with(Serdes.Long(), Serdes.String()));
	}

}
