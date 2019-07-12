package Project2.flink_operators;

import Project2.entity.RedisBean;
import Project2.flink.RedisConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class Query3AggregateComments23 implements AggregateFunction<Tuple3<Long, Long, Long>, Tuple5<Long, Double, Double, Long, Long>, Tuple5<Long, Double, Double, Long, Long>> {
    @Override
    public Tuple5<Long, Double, Double, Long, Long> createAccumulator() {
        //commentid indirectCount #likes userid inReplyTo
        return new Tuple5<>(0L, 0.0, 0.0, 0L, 0L);
    }

    @Override
    public Tuple5<Long, Double, Double, Long, Long> add(Tuple3<Long, Long, Long> tuple, Tuple5<Long, Double, Double, Long, Long> acc) {
        acc.f0 = tuple.f0;
        if (tuple.f2 == 0L) {
            acc.f1 = tuple.f1.doubleValue();
        } else {
            acc.f3 = tuple.f1;
            acc.f4 = tuple.f2;
        }
        return acc;
    }

    /**
     * If the user Id is not set, the comments is referred to a comment in a previous window, it is necessary to recall
     * this value from Redis
     */
    @Override
    public Tuple5<Long, Double, Double, Long, Long> getResult(Tuple5<Long, Double, Double, Long, Long> res) {
        //commentid indirectCount #likes userid inReplyTo
        if (res.f3.equals(0L)) {
            ObjectMapper mapper = new ObjectMapper();
            Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
            String response = jedis.get(String.valueOf(res.f0));
            jedis.close();
            if (response != null) {
                RedisBean redisBean = null;
                try {
                    redisBean = mapper.readValue(response, RedisBean.class);
                    if (redisBean.getInReplyto() == 0L)
                        return null;
                    res.f3 = redisBean.getUserId();
                    res.f4 = redisBean.getInReplyto();
                    //System.err.println(redisBean);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                return null;
            }
        }
        return res;
    }

    @Override
    public Tuple5<Long, Double, Double, Long, Long> merge(Tuple5<Long, Double, Double, Long, Long> acc1, Tuple5<Long, Double, Double, Long, Long> acc2) {
        return new Tuple5<>(acc1.f0,
                acc1.f1 == 0 ? acc2.f1 : acc1.f1,
                acc1.f2 == 0.0 ? acc2.f2 : acc1.f2,
                acc1.f3 == 0 ? acc2.f3 : acc1.f3,
                acc1.f4 == 0 ? acc2.f4 : acc1.f4);
    }
}
