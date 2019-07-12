package Project2.flink;

import Project2.deserializer.Tuple2Serializer;
import Project2.flink_operators.MonthlyWindow;
import Project2.flink_operators.Query2AddTimeSlot;
import Project2.flink_operators.Query2Aggregate;
import Project2.flink_operators.Query2ApplyTimeSlotSort;
import Project2.flink_operators.Query2Map;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Properties;

/**
 * Query2 implementation: returns the number of direct comments in every two-hours time slot. Results are emitted
 * every 24 hours, 7 days and 30 days
 */
public class Query2 {
    public static void doQuery(DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>> dataStream, Properties configProperties) throws InterruptedException {


        //Return a tuple composed by <creationDate, commentType>
        DataStream<Tuple2<Long, String>> projectDs = dataStream
                .project(5, 4);

        //Filter all direct comment
        DataStream<Tuple1<Long>> filteredDs = projectDs
                .filter(tuple -> tuple.f1.equals("comment"))
                //emit a tuple with 1 counter
                .map(new Query2Map());


        DataStream<Tuple2<Integer, Long>> stream1D = filteredDs
                //aggregate results every 2 hours
                .windowAll(TumblingEventTimeWindows.of(Time.hours(2)))
                //count the comments number in the time slot and emit a tuple with the time slot information
                .aggregate(new Query2Aggregate(), new Query2AddTimeSlot());

        DataStream<Tuple2<Integer, Long>> dailyStream = stream1D
                //key by time slot
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                //aggregate over time slot
                .sum(1);

       dailyStream.windowAll(TumblingEventTimeWindows.of(Time.days(1)))
               //sort the tuple on time slot identifier
                .apply(new Query2ApplyTimeSlotSort())
               //emit the result on kafka
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Integer, Long>>>>("query2_24_hours", new Tuple2Serializer<Integer, Long>(), configProperties));
			   //.writeAsText("query2_24_hours", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);


		DataStream<Tuple2<Integer, Long>> weeklyStream = dailyStream.keyBy(0)
                //aggregate values over 7 days
                .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                .sum(1);

        weeklyStream.windowAll(TumblingEventTimeWindows.of(Time.days(7), Time.days(-3)))
                //sort the tuple on time slot identifier
                .apply(new Query2ApplyTimeSlotSort())
                //emit the result on kafka
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Integer, Long>>>>("query2_7_days", new Tuple2Serializer<Integer, Long>(), configProperties));
				//.writeAsText("query2_7_days", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);

        // Query2 aggregation with tumbling windows over 30 days (alternative implementation)
        /*weeklyStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(30), Time.days(-3)))
                .sum(1)
                .windowAll(TumblingEventTimeWindows.of(Time.days(30), Time.days(-3)))
                .apply(new Query2ApplyTimeSlotSort())
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Integer, Long>>>>("query2_30_days", new Tuple2Serializer<Integer, Long>(), configProperties));*/

        //Aggregation over monthly custom window. Results are emitted every month
        dailyStream.keyBy(0)
                .window(new MonthlyWindow())
                .sum(1)
                .windowAll(new MonthlyWindow())
                .apply(new Query2ApplyTimeSlotSort())
                //.addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Integer, Long>>>>("query2_30_days", new Tuple2Serializer<Integer, Long>(), configProperties));
				.writeAsText("query2_1_month", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);


	}
}
