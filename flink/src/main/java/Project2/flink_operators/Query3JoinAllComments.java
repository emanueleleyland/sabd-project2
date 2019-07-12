package Project2.flink_operators;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Query3JoinAllComments implements CoMapFunction<Tuple4<Long, Double, Double, Long>, Tuple5<Long, Double, Double, Long, Long>, Tuple4<Long, Double, Double, Long>> {
    //commentId #likes IndirectCounts userId
    @Override
    public Tuple4<Long, Double, Double, Long> map1(Tuple4<Long, Double, Double, Long> tuple) throws Exception {
        return tuple;
    }

    //commentid indirectCount #likes userid inReplyTo
    @Override
    public Tuple4<Long, Double, Double, Long> map2(Tuple5<Long, Double, Double, Long, Long> tuple) throws Exception {
        return new Tuple4<>(tuple.f4, -1.0, tuple.f1, tuple.f3);
    }
}
