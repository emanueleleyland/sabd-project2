package Project2.flink;

import Project2.entity.RedisBean;
import Project2.flink_operators.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Query3 implementation: returns the 10 most popular users. The popularity score is based on the
 * likes received for each comment and the number of replies received by other users.
 * Results are emitted every day, week and every month
 */
public class Query3 {

	public static void doQuery(DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>> dataStream, Properties configProperties) {

		//Returns a tuple composed by the following fields:
		//commentid, commentType, editorSelection, recommendations, userId, depth, inReplyTo
		DataStream<Tuple7<Long, String, Boolean, Long, Long, Long, Long>> directComments = dataStream.project(3, 4, 7, 10, 13, 6, 8);

		//Returns a DataStream of direct comments (depth = 1)
		//with fields: commentId, recommendation, userId
		DataStream<Tuple3<Long, Double, Long>> comments_1 = directComments
				//filter depth = 1
				.filter(t -> t.f5.equals(1L))
				//Increment recommendation if the comments is selected by the editor
				.map(new Query3MapApproveIncr())
				.keyBy(2)
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				//store the tuple on Redis DB
				.apply(new Query3ApplyDirectComments());


		//Returns a DataStream of indirect comments (depth = 2)
		//with: commentId userId inReplyTo
		DataStream<Tuple3<Long, Long, Long>> comments_2 = directComments
				.filter(t -> t.f5.equals(2L))
				.map(new Query3MapIndirectComments())
				.keyBy(2)
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				//Store the tuple on Redis
				.apply(new Query3ApplyIndirectComments());


		//Return a DataStream of indirect comments with fields:
		//inreplyTo commentType
		DataStream<Tuple2<Long, String>> indirectComments = dataStream.project(8, 4);

		indirectComments = indirectComments.filter(t -> t.f1.equals("userReply"));

		//Compute the number of indirect comments referred to each comment
		//Emit a datastream with fields: inReplyTo, #indirectComments
		DataStream<Tuple2<Long, Long>> indirectSum = indirectComments
				.map(new MapFunction<Tuple2<Long,String>, Tuple2<Long, Long>>() {
					@Override
					public Tuple2<Long, Long> map(Tuple2<Long, String> tuple2) throws Exception {
						return new Tuple2<>(tuple2.f0, 1L);
					}
				})
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				.sum(1);

		//doQueryTime(comments_1, comments_2, indirectSum, 1L, "query3_24_hours");
		//doQueryTime(comments_1, comments_2, indirectSum, 7L, "query3_7_days");
		doQueryTime(comments_1, comments_2, indirectSum, 30L, "query3_1_month");
	}

	private static void doQueryTime(DataStream<Tuple3<Long, Double, Long>> comments_1, DataStream<Tuple3<Long, Long, Long>> comments_2, DataStream<Tuple2<Long, Long>> indirectSum, Long days, String topic){

		//Join indirectComments Datastream with depth-2 comments
		//Returns a datastream with fields:
		//commentid #IndirectComments #likes userid inReplyTo
		DataStream<Tuple5<Long, Double, Double, Long, Long>> comments_2_indirect = indirectSum
				.connect(comments_2)
				.map(new Query3Comments23Map())
				.keyBy(0)
				.window(days == 30L ? new MonthlyWindow() : days == 7L ? TumblingEventTimeWindows.of(Time.days(days), Time.days(-3)) : TumblingEventTimeWindows.of(Time.days(days)))
				.aggregate(new Query3AggregateComments23());

		//Join indirectComments Datastream with depth-1 comments
		//Returns a datastream with fields:
		//commentid #IndirectComments #likes userid
		DataStream<Tuple4<Long, Double, Double, Long>> comments_1_indirect = indirectSum
				.connect(comments_1)
				.map(new Query3Comments12Map())
				.keyBy(0)
				.window(days == 30L ? new MonthlyWindow() : TumblingEventTimeWindows.of(Time.days(days)))
				.aggregate(new Query3AggregateComments12());


		DataStream<Tuple4<Long, Double, Double, Long>> comment_1_final = comments_1_indirect
				.connect(comments_2_indirect)
				.map(new Query3JoinAllComments())
				.keyBy(0)
				.window(days == 30L ? new MonthlyWindow() : days == 7L ? TumblingEventTimeWindows.of(Time.days(days), Time.days(-3)) : TumblingEventTimeWindows.of(Time.days(days)))
				.aggregate(new Query3AllCommentsAggregate());


		DataStream<Tuple3<Long, Long, Double>> aggregateStreams = comment_1_final
				.connect(comments_2_indirect)
				.map(new CoMapFunction<Tuple4<Long, Double, Double, Long>, Tuple5<Long, Double, Double, Long, Long>, Tuple3<Long, Double, Double>>() {
					//commentId #likes IndirectCounts userId
					@Override
					public Tuple3<Long, Double, Double> map1(Tuple4<Long, Double, Double, Long> tuple) throws Exception {
						return new Tuple3<>(tuple.f3, tuple.f1, tuple.f2);
					}

					//commentid indirectCount #likes userid inReplyTo
					@Override
					public Tuple3<Long, Double, Double> map2(Tuple5<Long, Double, Double, Long, Long> tuple) throws Exception {
						return new Tuple3<>(tuple.f3, tuple.f2, tuple.f1);
					}
				})
				.keyBy(0)
				.window(days == 30L ? new MonthlyWindow() : days == 7L ? TumblingEventTimeWindows.of(Time.days(days), Time.days(-3)) : TumblingEventTimeWindows.of(Time.days(days)))
				//Compute, for each user, his score combining recommendations and replies
				.aggregate(new Query3UserScore());

		//Split the aggregated datastream for each taskmanager and emit a partial rank with at most 10 elements every day
		aggregateStreams
				.keyBy(0)
				.window(days == 30L ? new MonthlyWindow() : days == 7L ? TumblingEventTimeWindows.of(Time.days(days), Time.days(-3)) : TumblingEventTimeWindows.of(Time.days(days)))
				.aggregate(new Query3PartialRank())
				.windowAll(days == 30L ? new MonthlyWindow() : days == 7L ? TumblingEventTimeWindows.of(Time.days(days), Time.days(-3)) : TumblingEventTimeWindows.of(Time.days(days)))
				//Compute total ranking sending all the partial ranks to one worker node
				.process(new Query3Ranking())
				//.addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Long, Double>>>>("query3_24_hours", new Tuple2Serializer<Long, Double>(), configProperties));
				.writeAsText(topic, FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);
	}

}
