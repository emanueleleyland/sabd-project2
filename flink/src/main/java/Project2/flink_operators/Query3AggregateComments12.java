package Project2.flink_operators;

import Project2.entity.RedisBean;
import Project2.flink.RedisConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import redis.clients.jedis.Jedis;

import java.io.IOException;

public class Query3AggregateComments12 implements AggregateFunction<Tuple4<Long, Double, Long, Long>, Tuple4<Long, Double, Double, Long>, Tuple4<Long, Double, Double, Long>> {
    @Override
    public Tuple4<Long, Double, Double, Long> createAccumulator() {
        return new Tuple4<>(0L, 0.0, 0.0, 0L);
    }

    @Override
    public Tuple4<Long, Double, Double, Long> add(Tuple4<Long, Double, Long, Long> tuple, Tuple4<Long, Double, Double, Long> acc) {
        acc.f0 = tuple.f0;
        if (tuple.f3 == 0L) {
            acc.f2 = tuple.f2.doubleValue();
        } else {
            acc.f1 = tuple.f1;
            acc.f3 = tuple.f3;
        }
        return acc;
    }

    /**
     * If the user Id is not set, the comments is referred to a comment in a previous window, it is necessary to recall
     * this value from Redis
     */
    @Override
    public Tuple4<Long, Double, Double, Long> getResult(Tuple4<Long, Double, Double, Long> res) {
        if (res.f3 == 0) {
            ObjectMapper mapper = new ObjectMapper();
            Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
            String response = jedis.get(String.valueOf(res.f0));
            jedis.close();
            if (response != null) {
                RedisBean redisBean = null;
                try {
                    redisBean = mapper.readValue(response, RedisBean.class);
                    if (redisBean.getInReplyto() != 0L)
                        return null;
                    res.f3 = redisBean.getUserId();
                    //res.f1 = redisBean.getRecommendations();
                    res.f1 = 0.0;
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
    public Tuple4<Long, Double, Double, Long> merge(Tuple4<Long, Double, Double, Long> acc1, Tuple4<Long, Double, Double, Long> acc2) {
        return new Tuple4<>(acc1.f0,
                acc1.f1 == 0 ? acc2.f1 : acc1.f1,
                acc1.f2 == 0.0 ? acc2.f2 : acc1.f2,
                acc1.f3 == 0 ? acc2.f3 : acc1.f3);
    }
}
