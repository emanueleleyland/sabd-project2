package Project2.flink;

import Project2.deserializer.Tuple2Serializer;
import Project2.flink_operators.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
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
 * Query 1 implementation: Returns the ranking of the three most popular articles
 * i.e. those who received the highest number of comments (direct or indirect).
 * Results are emitted every hour, day and week (of event time)
 */
public class Query1 {

    public static void doQuery(DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean,
            Long, String, Long, String, String, Long, String>> inputStream, Properties configProperties) {


        //Emit a tuple composed only by the article ID
        DataStream<Tuple1<String>> filterDs =
                inputStream.project(1);



        DataStream<Tuple3<Long, String, Long>> hourlyStream = filterDs
                //key by article id
                .keyBy(0)
                //Process the value over a Tumbling window
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //Count the numbers of comment referred to the given article
                .aggregate(new Query1ArticleAggregator());

        hourlyStream
                //key by a pseudo random number between 1 and # of task managers
                //to compute a partial distributed ranking
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //Compute partial rank
                .aggregate(new Query1AggregatePartialRank())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                //emit the global rank
                .process(new Query1ProcessRank())
                //write results on kafka
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<String, Long>>>>("query1_1_hour", new Tuple2Serializer<String, Long>(), configProperties));
                //.writeAsText("query1_1_hour", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);


        DataStream<Tuple3<Long, String, Long>> dailyStream = hourlyStream
                .keyBy(1)
                //aggregates the hourly stream over 1 day
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .aggregate(new Query1Aggregate());

        dailyStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                //Compute partial rank over 1 day data
                .aggregate(new Query1AggregatePartialRank())
                .windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                //emit the global rank
                .process(new Query1ProcessRank())
                //write results on kafka
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<String, Long>>>>("query1_1_day", new Tuple2Serializer<String, Long>(), configProperties));
        //.writeAsText("query1_1_day", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);


        dailyStream
                .keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                //Compute partial rank over 1 day data
                .aggregate(new Query1Aggregate())
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                //Compute partial rank over 7 days data
                .aggregate(new Query1AggregatePartialRank())
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                //emit the global rank
                .process(new Query1ProcessRank())
                //write results on kafka
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<String, Long>>>>("query1_7_days", new Tuple2Serializer<String, Long>(), configProperties));
                //.writeAsText("query1_7_days", FileSystem.WriteMode.NO_OVERWRITE).setParallelism(1);
    }

}
