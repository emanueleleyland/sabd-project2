package Project2.flink_operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class Query2Map implements MapFunction<Tuple2<Long, String>, Tuple1<Long>> {
    @Override
    public Tuple1<Long> map(Tuple2<Long, String> tuple2) throws Exception {
        return new Tuple1<>(1L);
    }
}
