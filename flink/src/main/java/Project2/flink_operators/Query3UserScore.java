package Project2.flink_operators;

import Project2.flink.FlinkConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Random;

public class Query3UserScore implements AggregateFunction<Tuple3<Long, Double, Double>, Tuple3<Long, Double, Double>, Tuple3<Long, Long, Double>> {
    private static double wa = 0.3, wb = 0.7;
    private Random random = new Random(FlinkConfig.SEED);

    @Override
    public Tuple3<Long, Double, Double> createAccumulator() {
        //userid #likes indirectCounts
        return new Tuple3<>(0L, 0.0, 0.0);
    }

    //Aggregates the recommendation values and the number of indirect comments references
    @Override
    public Tuple3<Long, Double, Double> add(Tuple3<Long, Double, Double> tuple, Tuple3<Long, Double, Double> acc) {
        acc.f0 = tuple.f0;
        acc.f1 += tuple.f1;
        acc.f2 += tuple.f2;
        return acc;
    }

    //Emit a tuple prepending a random value between 0 and # of Parallelism set in flink configuration
    @Override
    public Tuple3<Long, Long, Double> getResult(Tuple3<Long, Double, Double> tuple) {
        return new Tuple3<>((long) random.nextInt(FlinkConfig.PARALLELISM), tuple.f0, tuple.f1 * wa + tuple.f2 * wb);
    }

    @Override
    public Tuple3<Long, Double, Double> merge(Tuple3<Long, Double, Double> acc1, Tuple3<Long, Double, Double> acc2) {
        acc1.f0 = acc2.f0;
        acc1.f1 += acc2.f1;
        acc1.f2 += acc2.f2;
        return acc1;
    }
}
