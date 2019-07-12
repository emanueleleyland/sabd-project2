package Project2.flink_operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Query2ApplyTimeSlotSort implements AllWindowFunction<Tuple2<Integer, Long>, Tuple2<Long, List<Tuple2<Integer, Long>>>, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple2<Integer, Long>> iterable, Collector<Tuple2<Long, List<Tuple2<Integer, Long>>>> collector) throws Exception {
        //Sort the tuple on time slot field
        ArrayList<Tuple2<Integer, Long>> list = new ArrayList<>();
        iterable.iterator().forEachRemaining(t -> list.add(t));
        list.sort(Comparator.comparing(o -> o.f0));
        //Emit the results as window start timestamp and list of <timeslot ID, # comments>
        collector.collect(new Tuple2<>(timeWindow.getStart(), list));
    }
}
