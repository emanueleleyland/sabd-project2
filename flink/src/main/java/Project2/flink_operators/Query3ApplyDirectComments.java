package Project2.flink_operators;

import Project2.entity.RedisBean;
import Project2.flink.RedisConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

public class Query3ApplyDirectComments implements WindowFunction<Tuple3<Long, Double, Long>, Tuple3<Long, Double, Long>, Tuple, TimeWindow> {
    /**
     * Save the tuple on Redis Server. If an indirect comment refers to it in a future window,
     * it has to be possible to recall the tuple, to get the user ID.
     */
    @Override
    public void apply(Tuple t, TimeWindow
            timeWindow, Iterable<Tuple3<Long, Double, Long>> iterable, Collector<Tuple3<Long, Double, Long>> collector) throws
            Exception {
        Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
        ObjectMapper mapper = new ObjectMapper();
        iterable.forEach(tuple -> {
            try {
                jedis.set(tuple.f0.toString(), mapper.writeValueAsString(new RedisBean(tuple.f2, tuple.f1, 0L)));
                jedis.expire(tuple.f0.toString(), RedisConfig.expirationTime);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            collector.collect(tuple);
        });
        jedis.close();
    }

}
