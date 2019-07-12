package Project2.flink_operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;

public class Query3MapApproveIncr implements MapFunction<Tuple7<Long, String, Boolean, Long, Long, Long, Long>, Tuple3<Long, Double, Long>> {

    private static double approveIncr = 1.1;

    //increments the recommendation value whether the comment is approved by editor
    @Override
    public Tuple3<Long, Double, Long> map(Tuple7<Long, String, Boolean, Long, Long, Long, Long> tuple) throws Exception {
        if (tuple.f2)
            return new Tuple3<>(tuple.f0, tuple.f3 * approveIncr, tuple.f4);
        else
            return new Tuple3<>(tuple.f0, tuple.f3.doubleValue(), tuple.f4);
    }
}
