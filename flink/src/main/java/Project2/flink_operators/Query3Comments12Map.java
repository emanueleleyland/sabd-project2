package Project2.flink_operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Query3Comments12Map implements CoMapFunction<Tuple2<Long, Long>, Tuple3<Long, Double, Long>, Tuple4<Long, Double, Long, Long>> {
    //inReplyTo #indirectComments
    @Override
    public Tuple4<Long, Double, Long, Long> map1(Tuple2<Long, Long> tuple2) throws Exception {
        return new Tuple4<>(tuple2.f0, 0.0, tuple2.f1, 0L);
    }

    //commentId #likes userId
    @Override
    public Tuple4<Long, Double, Long, Long> map2(Tuple3<Long, Double, Long> tuple3) throws Exception {
        return new Tuple4<>(tuple3.f0, tuple3.f1, 0L, tuple3.f2);
    }
}
