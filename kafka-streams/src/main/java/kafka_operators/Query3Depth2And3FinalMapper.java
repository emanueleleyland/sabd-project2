package kafka_operators;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.CommentTupleByUser;
import entity.RedisBean;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import redis.clients.jedis.Jedis;
import utils.SerializerAny;

import java.io.IOException;

public class Query3Depth2And3FinalMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Long, byte[]>> {
	private ObjectMapper mapper;
	private Jedis jedis;

	public Query3Depth2And3FinalMapper(Jedis jedis, ObjectMapper mapper) {
		this.jedis = jedis;
		this.mapper = mapper;
	}

	/**
	 * If the user Id is not set, the comments is referred to a comment in a previous window, it is necessary to recall
	 * this value from Redis
	 */
	@Override
	public KeyValue<Long, byte[]> apply(Windowed<Long> k, byte[] v) {
		CommentTupleByUser commentTupleByUser = (CommentTupleByUser) SerializerAny.deserialize(v);

		if (commentTupleByUser.userId.equals(0L)) {

			String response = jedis.get(String.valueOf(k.key()));
			if (response != null) {
				RedisBean redisBean = null;
				try {
					redisBean = mapper.readValue(response, RedisBean.class);
					if (redisBean.getInReplyto() == 0L)
						return null;
					commentTupleByUser.userId = redisBean.getUserId();
					commentTupleByUser.inReplyTo = redisBean.getInReplyto();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				return new KeyValue<>(null, null);
			}
		}

		v = SerializerAny.serialize(commentTupleByUser);
		return new KeyValue<>(k.key(), v);
	}
}
