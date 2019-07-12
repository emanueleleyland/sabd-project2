package Project2.flink_operators;

import Project2.flink.FlinkConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Random;

public class Query1ArticleAggregator implements AggregateFunction<Tuple1<String>, Tuple2<String, Long>, Tuple3<Long, String, Long>> {

    //return an empty aggregator (ArticleId, 0L)
    @Override
    public Tuple2<String, Long> createAccumulator() {
        return new Tuple2<>("", 0L);
    }

    @Override
    public Tuple2<String, Long> add(Tuple1<String> tuple, Tuple2<String, Long> acc) {
        //set the article ID and increment the number of occurrences
        acc.f1++;
        acc.f0 = tuple.f0;
        return acc;
    }

    @Override
    public Tuple3<Long, String, Long> getResult(Tuple2<String, Long> res) {
        //emit the result of aggregation prepending to the tuple a random number
        //between 1 and # of task manager to allow a distributed sorting of the ranking
        Random random = new Random();
        return new Tuple3<>((long) random.nextInt(FlinkConfig.PARALLELISM), res.f0, res.f1);
    }

    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
        return new Tuple2<>(acc1.f0, acc1.f1 + acc2.f1);
    }
}
