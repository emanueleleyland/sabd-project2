package Project2.metrics;

import Project2.deserializer.Tuple2Serializer;
import Project2.entity.Comment;
import Project2.flink_operators.*;
import Project2.flink_operators_metrics.Query2MapMetrics;
import com.codahale.metrics.SlidingWindowReservoir;
import org.apache.flink.api.java.tuple.*;
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

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Properties;

public class Query2Metrics {

    public static void doQuery(DataStream<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>> dataStream, Properties configProperties) throws InterruptedException {

        // ---- for metric computation only ----
        DataStream<Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>> metricStream = dataStream.process(new ProcessFunction<Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String>, Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>>() {
            private transient Meter meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                this.meter = getRuntimeContext().getMetricGroup().addGroup("Query2").meter("throughput_in", new DropwizardMeterWrapper(dropwizard));
            }

            @Override
            public void processElement(Tuple15<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String> t, Context context, Collector<Tuple16<Long, String, Long, Long, String, Long, Long, Boolean, Long, String, Long, String, String, Long, String, Long>> collector) throws Exception {
                this.meter.markEvent();
                Long start = NanoTimestamp.getNanoTimestamp();
                collector.collect(new Tuple16<>(t.f0, t.f1, t.f2, t.f3, t.f4, t.f5, t.f6, t.f7, t.f8, t.f9, t.f10, t.f11, t.f12, t.f13, t.f14, start));
            }

        });


        //Timestamp, commentType
        DataStream<Tuple3<Long, String, Long>> projectDs = metricStream
                .project(5, 4, 15);

        DataStream<Tuple1<Long>> filteredDs = projectDs
                .filter(tuple -> tuple.f1.equals("comment"))
                .map(new Query2MapMetrics())
                .process(new ProcessFunction<Tuple2<Long, Long>, Tuple1<Long>>() {
                    private transient Meter meter;
                    private transient DropwizardHistogramWrapper histogram;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
                        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query2").meter("thoughput_pre_window", new DropwizardMeterWrapper(dropwizard));
                        com.codahale.metrics.Histogram dropwizardHistogram =
                                new com.codahale.metrics.Histogram(new SlidingWindowReservoir(500));

                        this.histogram = getRuntimeContext()
                                .getMetricGroup()
                                .addGroup("Query2")
                                .histogram("histogram", new DropwizardHistogramWrapper(dropwizardHistogram));

                    }

                    @Override
                    public void processElement(Tuple2<Long, Long> tuple2, Context context, Collector<Tuple1<Long>> collector) throws Exception {
                        this.meter.markEvent();
                        Long end = NanoTimestamp.getNanoTimestamp();
                        Long processingTime = end - tuple2.f1;
                        this.histogram.update(processingTime);
                        collector.collect(new Tuple1<>(tuple2.f0));
                    }
                });


        DataStream<Tuple2<Integer, Long>> stream1D = filteredDs
                .windowAll(TumblingEventTimeWindows.of(Time.hours(2)))
                .aggregate(new Query2Aggregate(), new Query2AddTimeSlot());

        DataStream<Tuple2<Integer, Long>> dailyStream = stream1D.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .sum(1);

        dailyStream.windowAll(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new Query2ApplyTimeSlotSort())
                .addSink(new FlinkKafkaProducer011<Tuple2<Long, List<Tuple2<Integer, Long>>>>("query2_24_hours", new Tuple2Serializer<Integer, Long>(), configProperties));


    }


}
