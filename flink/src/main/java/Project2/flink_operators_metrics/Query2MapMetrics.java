package Project2.flink_operators_metrics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Query2MapMetrics implements MapFunction<Tuple3<Long, String, Long>, Tuple2<Long, Long>> {
    @Override
    public Tuple2<Long, Long> map(Tuple3<Long, String, Long> tuple3) throws Exception {
        return new Tuple2<>(1L, tuple3.f2);
    }
}
