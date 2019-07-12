package Project2.flink_operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Query1AggregatePartialRank implements AggregateFunction<Tuple3<Long, String, Long>, List<Tuple2<String, Long>>, Tuple1<List<Tuple2<String, Long>>>> {
    //return a new empty ranking list
    @Override
    public List<Tuple2<String, Long>> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Tuple2<String, Long>> add(Tuple3<Long, String, Long> tuple3, List<Tuple2<String, Long>> list) {
        //add a new record to the list (Article id, # of comments)
        list.add(new Tuple2<>(tuple3.f1, tuple3.f2));
        return list;
    }

    @Override
    public Tuple1<List<Tuple2<String, Long>>> getResult(List<Tuple2<String, Long>> list) {
        //sort the list and emit the first three article rank
        list.sort(Comparator.comparing(o -> -o.f1));
        return new Tuple1<>(new ArrayList<>(list.subList(0, Math.min(3, list.size()))));
    }

    @Override
    public List<Tuple2<String, Long>> merge(List<Tuple2<String, Long>> list1, List<Tuple2<String, Long>> list2) {
        list1.forEach(t -> list2.add(t));
        return list2;
    }
}
