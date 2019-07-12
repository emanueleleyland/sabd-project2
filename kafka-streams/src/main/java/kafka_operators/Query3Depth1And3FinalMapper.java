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

public class Query3Depth1And3FinalMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>> {
	private Jedis jedis;
	private ObjectMapper mapper;

	public Query3Depth1And3FinalMapper(Jedis jedis, ObjectMapper mapper) {
		this.jedis = jedis;
		this.mapper = mapper;
	}

	@Override
	public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> k, byte[] v) {
		CommentTupleByUser commentTupleByUser = (CommentTupleByUser) SerializerAny.deserialize(v);

		if (commentTupleByUser.userId.equals(0L)) {

			String response = jedis.get(String.valueOf(k.key()));
			if (response != null) {
				RedisBean redisBean = null;
				try {
					redisBean = mapper.readValue(response, RedisBean.class);
					if (redisBean.getInReplyto() != 0L)
						return new KeyValue<>(null, null);
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
		return new KeyValue<>(k, v);
	}
}
