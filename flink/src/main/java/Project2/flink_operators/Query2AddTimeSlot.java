package Project2.flink_operators;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class Query2AddTimeSlot extends ProcessAllWindowFunction<Tuple1<Long>, Tuple2<Integer, Long>, TimeWindow> {
    @Override
    public void process(Context context, Iterable<Tuple1<Long>> iterable, Collector<Tuple2<Integer, Long>> collector) throws Exception {
        //return a tuple composed by time slot ID (integer between 0 and 12) and count of comments
        ZonedDateTime ts = Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.of("UTC"));
        collector.collect(new Tuple2<>(ts.getHour(), iterable.iterator().next().f0));
    }
}
