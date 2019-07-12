package Project2.flink_operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Query3PartialRank implements AggregateFunction<Tuple3<Long, Long, Double>, List<Tuple2<Long, Double>>, List<Tuple2<Long, Double>>> {
    @Override
    public List<Tuple2<Long, Double>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Tuple2<Long, Double>> add(Tuple3<Long, Long, Double> tuple3, List<Tuple2<Long, Double>> list) {
        list.add(new Tuple2<>(tuple3.f1, tuple3.f2));
        return list;
    }

    //Returns a sorted rank for each task manager, with at most 10 elements
    @Override
    public List<Tuple2<Long, Double>> getResult(List<Tuple2<Long, Double>> list) {
        list.sort(Comparator.comparing(o -> -o.f1));
        return new ArrayList<>(list.subList(0, Math.min(10, list.size())));
    }

    @Override
    public List<Tuple2<Long, Double>> merge(List<Tuple2<Long, Double>> list1, List<Tuple2<Long, Double>> list2) {
        list1.forEach(t -> list2.add(t));
        return list2;
    }
}
