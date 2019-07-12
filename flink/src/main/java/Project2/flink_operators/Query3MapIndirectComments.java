package Project2.flink_operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;

public class Query3MapIndirectComments implements MapFunction<Tuple7<Long, String, Boolean, Long, Long, Long, Long>, Tuple4<Long, Double, Long, Long>> {
    @Override
    public Tuple4<Long, Double, Long, Long> map(Tuple7<Long, String, Boolean, Long, Long, Long, Long> tuple) throws Exception {
        return new Tuple4<>(tuple.f0, 0.0, tuple.f4, tuple.f6);
    }
}
