package Project2.flink_operators;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class Query3AllCommentsAggregate implements AggregateFunction<Tuple4<Long, Double, Double, Long>, Tuple4<Long, Double, Double, Long>, Tuple4<Long, Double, Double, Long>> {
    @Override
    public Tuple4<Long, Double, Double, Long> createAccumulator() {
        //commentid #likes indirectCounts userId
        return new Tuple4<>(0L, 0.0, 0.0, 0L);
    }

    @Override
    public Tuple4<Long, Double, Double, Long> add(Tuple4<Long, Double, Double, Long> tuple, Tuple4<Long, Double, Double, Long> acc) {
        acc.f0 = tuple.f0;
        if (tuple.f1 != -1.0) {
            acc.f1 = tuple.f1;
        }
        acc.f3 = tuple.f3;
        acc.f2 += tuple.f2;
        return acc;
    }

    @Override
    public Tuple4<Long, Double, Double, Long> getResult(Tuple4<Long, Double, Double, Long> res) {
        return res;
    }

    @Override
    public Tuple4<Long, Double, Double, Long> merge(Tuple4<Long, Double, Double, Long> acc1, Tuple4<Long, Double, Double, Long> acc2) {
        return new Tuple4<>(acc1.f0,
                acc1.f1 == 0 ? acc2.f1 : acc1.f1,
                acc1.f2 == 0.0 ? acc2.f2 : acc1.f2,
                acc1.f3 == 0 ? acc2.f3 : acc1.f3);
    }
}
