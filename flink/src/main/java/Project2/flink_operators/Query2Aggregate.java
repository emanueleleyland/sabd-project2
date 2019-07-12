package Project2.flink_operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;

public class Query2Aggregate implements AggregateFunction<Tuple1<Long>, Tuple1<Long>, Tuple1<Long>> {
    @Override
    public Tuple1<Long> createAccumulator() {
        return new Tuple1<>(0L);
    }

    @Override
    public Tuple1<Long> add(Tuple1<Long> in, Tuple1<Long> acc) {
        acc.f0 += in.f0;
        return acc;
    }

    @Override
    public Tuple1<Long> getResult(Tuple1<Long> res) {
        return res;
    }

    @Override
    public Tuple1<Long> merge(Tuple1<Long> acc1, Tuple1<Long> acc2) {
        return new Tuple1<>(acc1.f0 + acc2.f0);
    }

}
