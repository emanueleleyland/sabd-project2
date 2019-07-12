package Project2.flink_operators_metrics;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Query1ProcessThroughputMetrics extends ProcessWindowFunction<Tuple3<Long, String, Long>, Tuple3<Long, String, Long>, Tuple, TimeWindow> {

    private transient Meter meter;

    @Override
    public void open(Configuration parameters) throws Exception {
        com.codahale.metrics.Meter dropwizard = new com.codahale.metrics.Meter();
        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput", new DropwizardMeterWrapper(dropwizard));
    }

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple3<Long, String, Long>> iterable, Collector<Tuple3<Long, String, Long>> collector) throws Exception {
        this.meter.markEvent();
        collector.collect(iterable.iterator().next());
    }
}
