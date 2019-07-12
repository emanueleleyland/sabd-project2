package Project2.flink_operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Query3Comments23Map implements CoMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> {

    //Input stream: inReplyTo #indirectComments
    @Override
    public Tuple3<Long, Long, Long> map1(Tuple2<Long, Long> tuple) throws Exception {
        return new Tuple3<>(tuple.f0, tuple.f1, 0L);
    }

    //Input stream: commentId userId inReplyTo
    @Override
    public Tuple3<Long, Long, Long> map2(Tuple3<Long, Long, Long> tuple) throws Exception {
        return tuple;
    }
}
