package Project2.flink_operators;

import Project2.flink.FlinkConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Random;

/**
 * Aggregate class to count the number of articles with the same ID
 */
public class Query1Aggregate implements AggregateFunction<Tuple3<Long, String, Long>, Tuple2<String, Long>, Tuple3<Long, String, Long>> {


    //return an empty aggregator (ArticleId, 0L)
    @Override
    public Tuple2<String, Long> createAccumulator() {
        return new Tuple2<>("", 0L);
    }

    @Override
    public Tuple2<String, Long> add(Tuple3<Long, String, Long> tuple3, Tuple2<String, Long> tuple2) {
        //set the article ID and increment the number of occurrences
        tuple2.f0 = tuple3.f1;
        tuple2.f1 += tuple3.f2;
        return tuple2;
    }

    @Override
    public Tuple3<Long, String, Long> getResult(Tuple2<String, Long> tuple) {
        //emit the result of aggregation prepending to the tuple a random number
        //between 1 and # of task manager to allow a distributed sorting of the ranking
        Random random = new Random();
        return new Tuple3<>((long) random.nextInt(FlinkConfig.PARALLELISM), tuple.f0, tuple.f1);
    }

    @Override
    public Tuple2<String, Long> merge(Tuple2<String, Long> acc1, Tuple2<String, Long> acc2) {
        acc1.f0 = acc2.f0;
        acc1.f1 += acc2.f1;
        return acc1;
    }
}