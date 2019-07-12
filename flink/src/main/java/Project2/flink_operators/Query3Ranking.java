package Project2.flink_operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Query3Ranking extends ProcessAllWindowFunction<List<Tuple2<Long, Double>>, Tuple2<Long, List<Tuple2<Long, Double>>>, TimeWindow> {
    //Sort the partial ranks in one global rank with at most 10 users
    @Override
    public void process(Context context, Iterable<List<Tuple2<Long, Double>>> iterable, Collector<Tuple2<Long, List<Tuple2<Long, Double>>>> collector) throws Exception {
        ArrayList<Tuple2<Long, Double>> res = new ArrayList<>();
        iterable.forEach(t -> t.forEach(tu -> res.add(new Tuple2<>(tu.f0, tu.f1))));
        res.sort(Comparator.comparing(o -> -o.f1));
        LocalDateTime localDateTime = Instant.ofEpochMilli(context.window().getStart()).atZone(ZoneId.of("UTC")).toLocalDateTime();
        System.err.println("Day " + localDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        collector.collect(new Tuple2<>(context.window().getStart(), new ArrayList<>(res.subList(0, Math.min(10, res.size())))));
    }
}
