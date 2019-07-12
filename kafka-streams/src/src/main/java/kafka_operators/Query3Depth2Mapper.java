package kafka_operators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Comment;
import entity.IndirectComments2;
import entity.RedisBean;
import kafka_streams.RedisConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import redis.clients.jedis.Jedis;
import utils.SerializerAny;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class Query3Depth2Mapper implements KeyValueMapper<String, Comment, KeyValue<Windowed<Long>, byte[]>> {

	private Double approvedInc = 0.0;
	private Jedis jedis = null;
	private String redisRoot = "";
	private ObjectMapper mapper = null;

	public Query3Depth2Mapper(Double approvedInc, Jedis jedis, String redisRoot, ObjectMapper mapper) {
		this.approvedInc = approvedInc;
		this.jedis = jedis;
		this.redisRoot = redisRoot;
		this.mapper = mapper;
	}
	/**
	 * Save the tuple on Redis Server. If an indirect comment (depth 3) refers to it in a future window,
	 * it has to be possible to recall the tuple, to get the user ID.
	 */
	@Override
	public KeyValue<Windowed<Long>, byte[]> apply(String k, Comment v) {
		Long userId = Long.valueOf(v.getUserID());
		Long commentId = Long.valueOf(v.getCommentID());
		RedisBean redisBean = null;
		redisBean = new RedisBean(userId, 0.0, Long.valueOf(v.getInReplyTo()));
		try {
			jedis.set(redisRoot + userId, mapper.writeValueAsString(redisBean));
			jedis.expire(redisRoot + userId, RedisConfig.expirationTime);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		LocalDateTime localDateTime = Instant.ofEpochMilli(Long.valueOf(v.getCreateDate()) * 1000).atZone(ZoneId.of("UTC")).toLocalDateTime();
		LocalDateTime from = localDateTime.withHour(0).withMinute(0).withSecond(0).withNano(0);
		LocalDateTime to = from.plusDays(1);
		long fromMillis = from.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
		long toMillis = to.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();
		Windowed<Long> window = new Windowed<>(commentId, new TimeWindow(fromMillis, toMillis));
		return new KeyValue<>(window, SerializerAny.serialize(new IndirectComments2(userId, Long.valueOf(v.getInReplyTo()))));

	}
}
