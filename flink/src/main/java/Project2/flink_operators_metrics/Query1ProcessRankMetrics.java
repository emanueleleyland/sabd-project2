package Project2.flink_operators_metrics;

import com.codahale.metrics.Meter;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Query1ProcessRankMetrics extends ProcessAllWindowFunction<Tuple1<List<Tuple2<String, Long>>>, Tuple2<Long, List<Tuple2<String, Long>>>, TimeWindow> {
    private transient DropwizardMeterWrapper meter;

    @Override
    public void open(Configuration parameters) throws Exception {
        Meter dropwizard = new Meter();
        this.meter = getRuntimeContext().getMetricGroup().addGroup("Query1").meter("throughput_out", new DropwizardMeterWrapper(dropwizard));
    }


    @Override
    public void process(Context context, Iterable<Tuple1<List<Tuple2<String, Long>>>> iterable, Collector<Tuple2<Long, List<Tuple2<String, Long>>>> collector) throws Exception {
        List<Tuple2<String, Long>> list = new ArrayList<>();
        iterable.forEach(t -> t.f0.forEach(list::add));
        list.sort(Comparator.comparing(o -> -o.f1));
        collector.collect(new Tuple2<>(context.window().getStart(), new ArrayList<>(list.subList(0, Math.min(3, list.size())))));
        this.meter.markEvent();
        //System.err.println(this.meter.getRate());

    }
}

