package Project2.metrics;

import Project2.deserializer.Tuple2Serializer;
import Project2.flink_operators_metrics.Query1AggregatePartialRankMetrics;
import Project2.flink_operators_metrics.Query1ArticleAggregatorMetrics;
import Project2.flink_operators_metrics.Query1ProcessRankMetrics;
import Project2.flink_operators_metrics.Query1ProcessThroughputMetrics;
import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.api.java.tuple.Tuple16;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

/**
 * Query1 class used to get system evaluation
 */
public class Query1Metrics {

    public static void doQuery(DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean,
            Long, String, Long, String, String, Long, String>> inputStream, Properties configProperties) {


        // ---- for metric computation only ----
        //Append to every tuple the value of timestamp of the ingestion time
        DataStream<Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>> metricStream = inputStream.process(new ProcessFunction<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>, Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>>() {
            //Register a meter to track input throughput
            private transient Meter meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
            }

            @Override
            public void processElement(Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> t, Context context, Collector<Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>> collector) throws Exception {
                this.meter.markEvent();
                Long start = NanoTimestamp.getNanoTimestamp();
                collector.collect(new Tuple16<>(t.f0, t.f1, t.f2, t.f3, t.f4, t.f5, t.f6, t.f7, t.f8, t.f9, t.f10, t.f11, t.f12, t.f13, t.f14, start));

            }

        });


        DataStream<Tuple2<String, Long>> filterDs =
                metricStream.project(1, 15);


        // ---- for metric computation only ----
        // Register a metric to track the throughput of system before tuple are aggregate in the window
        // and an histogram to track latency of the tuple before they are injected in a window
        filterDs = filterDs.process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            private transient Meter meter;
            private transient DropwizardHistogramWrapper histogram;


            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("thoughput_pre_window", new DropwizardMeterWrapper(dropwizard));
                com.codahale.metrics.Histogram dropwizardHistogram =
                        new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

                this.histogram = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("Query1")
                        .histogram("histogram", new DropwizardHistogramWrapper(dropwizardHistogram));

            }

            @Override
            public void processElement(Tuple2<String, Long> tuple1, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                this.meter.markEvent();
                //Get latency value using the information included in the tuple
                Long end = NanoTimestamp.getNanoTimestamp();
                Long processingTime = end - tuple1.f1;
                //Long processingTime = Duration.between(Instant.ofEpochMilli(0), Instant.now()).toNanos() - tuple1.f1;
                this.histogram.update(processingTime);
                collector.collect(tuple1);
            }
        });


        // ---- --------------------------- ----


        /*DataStream<Tuple2<String, Long>> aggregateDs = */

        DataStream<Tuple3<Long, String, Long>> hourlyStream = filterDs
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1ArticleAggregatorMetrics(),
                        new Query1ProcessThroughputMetrics());

        //DataStream<Tuple2<Long, List<Tuple2<String, Long>>>>hourlyRank =


        hourlyStream
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new Query1AggregatePartialRankMetrics())
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .process(new Query1ProcessRankMetrics())
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<String, Long>>>>("query1_1_hour", new Tuple2Serializer<String, Long>(), configProperties));

    }


}
