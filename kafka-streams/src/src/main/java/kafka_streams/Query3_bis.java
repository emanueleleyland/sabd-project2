package kafka_streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import redis.clients.jedis.Jedis;
import serdes.JsonPOJODeserializer;
import serdes.JsonPOJOSerializer;
import utils.SerializerAny;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class Query3_bis {

	private static Double wa = 0.3, wb = 0.7, approvedInc = 1.1;
	private static String redisRoot = "KS_";

	public static void doQuery(KStream<String, Comment> kStream) {

		/*Jedis jedis = new Jedis(RedisConfig.REDIS_ADDR, Integer.parseInt(RedisConfig.REDIS_PORT));
		ObjectMapper mapper = new ObjectMapper();

		Map<String, Object> serdeProps = new HashMap<>();

		Deserializer<DirectCommentTuple> directCommentDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", DirectCommentTuple.class);
		directCommentDeserializer.configure(serdeProps, false);

		Serializer<DirectCommentTuple> directCommentSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", DirectCommentTuple.class);
		directCommentSerializer.configure(serdeProps, false);

		Serde<DirectCommentTuple> directCommentSerde = Serdes.serdeFrom(directCommentSerializer, directCommentDeserializer);

		Deserializer<IndirectComments2> indirectComments2Deserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", IndirectComments2.class);
		indirectComments2Deserializer.configure(serdeProps, false);

		Serializer<IndirectComments2> indirectComments2Serializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", IndirectComments2.class);
		indirectComments2Serializer.configure(serdeProps, false);

		Serde<IndirectComments2> indirectComments2Serde = Serdes.serdeFrom(indirectComments2Serializer, indirectComments2Deserializer);

		Deserializer<FinalTuple> finalTupleDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", FinalTuple.class);
		indirectComments2Deserializer.configure(serdeProps, false);

		Serializer<FinalTuple> finalTupleSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", FinalTuple.class);
		indirectComments2Serializer.configure(serdeProps, false);

		Serde<FinalTuple> finalTupleSerde = Serdes.serdeFrom(finalTupleSerializer, finalTupleDeserializer);

		Deserializer<CommentTupleByUser> commentTupleByUserDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", CommentTupleByUser.class);
		indirectComments2Deserializer.configure(serdeProps, false);

		Serializer<CommentTupleByUser> commentTupleByUserSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", CommentTupleByUser.class);
		indirectComments2Serializer.configure(serdeProps, false);

		Serde<CommentTupleByUser> commentTupleByUserSerde = Serdes.serdeFrom(commentTupleByUserSerializer, commentTupleByUserDeserializer);

		Deserializer<CommentRank[]> commentDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", CommentRank[].class);
		commentDeserializer.configure(serdeProps, false);

		Serializer<CommentRank[]> commentSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", CommentRank[].class);
		commentSerializer.configure(serdeProps, false);

		Serde<CommentRank[]> serde = Serdes.serdeFrom(commentSerializer, commentDeserializer);

		/*KStream<String, Comment>[] forks = kStream.branch(
				(k, v) -> Long.valueOf(v.getDepth()).equals(1L),
				(k, v) -> Long.valueOf(v.getDepth()).equals(2L),//comments to comments
				(k, v) -> v.getCommentType().equals("userReply") //user replies
		);*/

	/*	KStream<Windowed<Long>, byte[]> comments_1 = kStream
				.filter((k, v) -> Long.valueOf(v.getDepth()).equals(1L))
				.map((k, v) -> {
					Long userId = Long.valueOf(v.getUserID());
					Long commentId = Long.valueOf(v.getCommentID());
					Long recommendations = Long.valueOf(v.getRecommendations());
					Boolean selection = Boolean.valueOf(v.getEditorsSelection());
					RedisBean redisBean = null;
					if (selection) {
						redisBean = new RedisBean(userId, recommendations.doubleValue() * approvedInc, Long.valueOf(v.getInReplyTo()));
					} else {
						redisBean = new RedisBean(userId, recommendations.doubleValue(), Long.valueOf(v.getInReplyTo()));
					}
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
					//System.err.println("1 " + window.key());
					return new KeyValue<>(window, SerializerAny.serialize(new DirectCommentTuple(userId, redisBean.getRecommendations())));
				});

		//OUTPUT: Tuple3 commentId #likes userId

		KStream<Windowed<Long>, byte[]> comments_2 = kStream
				.filter((k, v) -> Long.valueOf(v.getDepth()).equals(2L))
				.map((k, v) -> {
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
					//System.err.println("2 " + window.window());
					return new KeyValue<>(window, SerializerAny.serialize(new IndirectComments2(userId, Long.valueOf(v.getInReplyTo()))));
				});

		//OUTPUT comment2: Tuple3 commentId userId inReplyTo

		KStream<Windowed<Long>, byte[]> indirectStream = kStream
				.filter((k, v) -> v.getCommentType().equals("userReply"))
				.map((k, v) -> new KeyValue<>(Long.valueOf(v.getInReplyTo()), 1L))
				.groupByKey(Grouped.with(Serdes.Long(), Serdes.Long()))
				.windowedBy(TimeWindows.of(Duration.ofDays(1)))
				.reduce(Long::sum)
				.suppress(Suppressed.untilWindowCloses(unbounded()))
				.toStream()
				.map((k, v) -> {
					//System.err.println("3 " + k.key());
					return new KeyValue<>(k, SerializerAny.serialize(v));
				});

		/*comments_1.print(Printed.toSysOut());
		comments_2.print(Printed.toSysOut());
		indirectStream.print(Printed.toSysOut());*/
		//OUTPUT: Tuple2: inReplyTo(commentId) #indirectComments

		/*comments_1.to("comments_1", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()));
		indirectStream.to("indirectStream", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()));*/

	/*	KStream<Windowed<Long>, byte[]> indirectStream1 = indirectStream.mapValues(new ValueMapper<byte[], byte[]>() {
			@Override
			public byte[] apply(byte[] bytes) {
				return bytes;
			}
		});

		Joined<Windowed<Long>, byte[], byte[]> joined = Joined.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray(), Serdes.ByteArray());

		KStream<Windowed<Long>, byte[]> comments_2_indirect = comments_2
				.outerJoin(indirectStream, new ValueJoiner<byte[], byte[], byte[]>() {
					@Override
					public byte[] apply(byte[] bytes, byte[] bytes2) {
						IndirectComments2 indirectComments2 = null;
						Long indirectComments = null;
						CommentTupleByUser commentTupleByUser = new CommentTupleByUser(0L, 0.0, 0.0, 0L);
						if (bytes != null) {
							indirectComments2 = (IndirectComments2) SerializerAny.deserialize(bytes);
							commentTupleByUser.userId = indirectComments2.userId;
							commentTupleByUser.inReplyTo = indirectComments2.inReplyTo;
						}
						if (bytes2 != null) {
							indirectComments = (Long) SerializerAny.deserialize(bytes2);
							commentTupleByUser.indirectCount = indirectComments.doubleValue();
						}
						if (commentTupleByUser.inReplyTo == 0L) return null;
						//System.err.println(commentTupleByUser);
						return SerializerAny.serialize(commentTupleByUser);
					}
				}, JoinWindows.of(Duration.ofDays(1)), joined);

		//commentid indirectCount #likes userid inReplyTo*/

	/*	KStream<Windowed<Long>, byte[]> comments_1_indirect = comments_1
				.outerJoin(indirectStream1, new ValueJoiner<byte[], byte[], byte[]>() {
					@Override
					public byte[] apply(byte[] bytes, byte[] bytes2) {
						DirectCommentTuple directCommentTuple = null;
						Long indirectComments = null;
						CommentTupleByUser commentTupleByUser = new CommentTupleByUser(0L, 0.0, 0.0, 0L);
						if (bytes != null) {
							directCommentTuple = (DirectCommentTuple) SerializerAny.deserialize(bytes);
							commentTupleByUser.userId = directCommentTuple.userId;
							commentTupleByUser.recommendations = directCommentTuple.recommendations;
						}
						if (bytes2 != null) {
							indirectComments = (Long) SerializerAny.deserialize(bytes2);
							commentTupleByUser.indirectCount = indirectComments.doubleValue();
						}
						if (commentTupleByUser.userId == 0L) return null;
						return SerializerAny.serialize(commentTupleByUser);
					}
				}, JoinWindows.of(Duration.ofDays(1)), joined);

		//commentId #likes indirectCount userID

		KStream<Windowed<Long>, byte[]> comments_2_indirect_2 = comments_2_indirect
				.mapValues(new ValueMapper<byte[], byte[]>() {
					@Override
					public byte[] apply(byte[] bytes) {
						return bytes;
					}
				});

		KStream<Windowed<Long>, byte[]> comment_2_indirect_flipped = comments_2_indirect
				.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>>() {
					@Override
					public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
						if (bytes == null) return new KeyValue<>(null, null);
						CommentTupleByUser commentTupleByUser = (CommentTupleByUser) SerializerAny.deserialize(bytes);
						return new KeyValue<>(new Windowed<>(commentTupleByUser.inReplyTo, longWindowed.window()), bytes);
					}
				});


		KStream<Windowed<Long>, byte[]> comment_1_final =
				comments_1_indirect
						.outerJoin(comment_2_indirect_flipped, new ValueJoiner<byte[], byte[], byte[]>() {
							@Override
							public byte[] apply(byte[] bytes, byte[] bytes2) {
								CommentTupleByUserAgg agg = new CommentTupleByUserAgg(0L, 0.0, 0.0, 0L, 0.0);
								if (bytes != null) {
									CommentTupleByUser commentTupleByUser1 = (CommentTupleByUser) SerializerAny.deserialize(bytes);
									agg.indirectCount = commentTupleByUser1.indirectCount;
									agg.recommendations = commentTupleByUser1.recommendations;
									agg.userId = commentTupleByUser1.userId;
								}
								if (bytes2 != null) {
									CommentTupleByUser commentTupleByUser2 = (CommentTupleByUser) SerializerAny.deserialize(bytes2);
									agg.agg += wa * commentTupleByUser2.recommendations + wb * commentTupleByUser2.indirectCount;
								}
								if(agg.userId == 0L) return null;
								//System.err.println(agg);
								return SerializerAny.serialize(agg);
							}
						}, JoinWindows.of(Duration.ofDays(1)), joined)
						.mapValues(new ValueMapper<byte[], byte[]>() {
							@Override
							public byte[] apply(byte[] bytes) {
								return bytes;
							}
						})
						.groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
						.reduce(new Reducer<byte[]>() {
							@Override
							public byte[] apply(byte[] v1, byte[] v2) {
								CommentTupleByUserAgg c1 = (CommentTupleByUserAgg) SerializerAny.deserialize(v1);
								CommentTupleByUserAgg c2 = (CommentTupleByUserAgg) SerializerAny.deserialize(v2);
								return SerializerAny.serialize(new CommentTupleByUserAgg(c1.inReplyTo, c1.indirectCount, c1.recommendations, c1.userId, c1.agg + c2.agg));
							}
						})
						.toStream()
						.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>>() {
							@Override
							public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
								CommentTupleByUserAgg c = (CommentTupleByUserAgg) SerializerAny.deserialize(bytes);
								CommentRank finalTuple = new CommentRank(c.userId, c.agg + wa * c.recommendations + wb * c.indirectCount);
								//System.err.println(finalTuple);
								return new KeyValue<Windowed<Long>, byte[]>(new Windowed<>(c.userId, longWindowed.window()), SerializerAny.serialize(finalTuple));
							}
						});

		KStream<Windowed<Long>, byte[]> comment_2_final = comments_2_indirect_2
				.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>>() {
					@Override
					public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
						if(bytes == null) return new KeyValue<>(null, null);
						CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(bytes);
						CommentRank record = new CommentRank(c.userId, c.recommendations * wa + c.indirectCount * wb);
						//System.err.println(record);
						return new KeyValue<>(new Windowed<>(c.userId, longWindowed.window()), SerializerAny.serialize(record));
					}
				});

		comment_1_final
				.merge(comment_2_final)
				.mapValues(new ValueMapper<byte[], byte[]>() {
					@Override
					public byte[] apply(byte[] bytes) {
						return bytes;
					}
				})
				.groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.reduce(new Reducer<byte[]>() {
					@Override
					public byte[] apply(byte[] v1, byte[] v2) {
						CommentRank c1 = (CommentRank) SerializerAny.deserialize(v1);
						CommentRank c2 = (CommentRank) SerializerAny.deserialize(v2);

						return SerializerAny.serialize(new CommentRank(c1.userId, c1.score + c2.score));
					}
				})
				.toStream()
				.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>>() {
					@Override
					public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
						CommentRank cr = (CommentRank) SerializerAny.deserialize(bytes);
						//System.err.println(cr);
						return new KeyValue<>(new Windowed<>(1L, longWindowed.window()), bytes);
					}
				})
				.groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.aggregate(new Initializer<byte[]>() {
					@Override
					public byte[] apply() {
						CommentRank[] comments = new CommentRank[]
								{new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0)};
						System.err.println("aAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaaaaaaAAAAAAAAAAAAAAaaaaAAAAAAAAAAAA");
						return SerializerAny.serialize(comments);
					}
				}, new Aggregator<Windowed<Long>, byte[], byte[]>() {
					@Override
					public byte[] apply(Windowed<Long> aLong, byte[] bytes, byte[] agg) {
						CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(agg);
						CommentRank comment = (CommentRank) SerializerAny.deserialize(bytes);

						for (int i = 0; i < 10; i++) {

							if (comments[i] == null) {
								comments[i] = comment;
								break;
							} else {
								if (comments[i].score <= comment.score) {
									for (int j = 8; j >= i; j--) {
										comments[j + 1] = comments[j];
									}
									comments[i] = comment;
									break;
								}
							}
						}
						return SerializerAny.serialize(comments);
					}
				}, Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.toStream()
		.foreach((k, v) -> {
			System.err.println(k.window().start() + " - " + k.window().end());
			CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(v);
			System.err.println("------------------------------------------------------------------------------");
			for (int i = 0; i < comments.length; i++) {
				System.err.println(i + " " + comments[i].userId + " - " + comments[i].score);
			}
			System.err.println("------------------------------------------------------------------------------");

		});


		/*comments_1_indirect
				.merge(comment_2_indirect_flipped)
				.groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.aggregate(new Initializer<byte[]>() {
					@Override
					public byte[] apply() {
						System.err.println("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
						return SerializerAny.serialize(new CommentTupleByUser(0L, 0.0, 0.0, 0L));
					}
				}, new Aggregator<Windowed<Long>, byte[], byte[]>() {
					@Override
					public byte[] apply(Windowed<Long> aLong, byte[] bytes, byte[] agg) {

						CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(agg);
						CommentTupleByUser val = (CommentTupleByUser) SerializerAny.deserialize(bytes);
						System.err.println(c);

						c.userId = val.userId;
						if(val.recommendations != 0.0) {
							System.err.println(val.recommendations);
							c.recommendations += val.recommendations;
						}
						c.indirectCount += val.indirectCount;
						return SerializerAny.serialize(c);
					}
				}, Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.toStream()
				/*.mapValues(new ValueMapper<byte[], byte[]>() {
					@Override
					public byte[] apply(byte[] bytes) {
						CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(bytes);
						System.err.println(c);

						return bytes;
					}
				})*/;


		/*KStream<Windowed<Long>, byte[]> aggregateStream = comment_1_final
				.merge(comments_2_indirect_2)
				.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>>() {
					@Override
					public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> aLong, byte[] bytes) {
						if (bytes == null) return new KeyValue<>(null, null);
						CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(bytes);
						return new KeyValue<>(new Windowed<>(c.userId, aLong.window()), SerializerAny.serialize(new FinalTuple(c.recommendations, c.indirectCount)));
					}
				})
				.groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.aggregate(new Initializer<byte[]>() {
					@Override
					public byte[] apply() {
						return SerializerAny.serialize(new FinalTuple(0.0, 0.0));
					}
				}, new Aggregator<Windowed<Long>, byte[], byte[]>() {
					@Override
					public byte[] apply(Windowed<Long> aLong, byte[] bytes, byte[] acc) {
						FinalTuple finalAcc = (FinalTuple) SerializerAny.deserialize(acc);
						FinalTuple tuple = (FinalTuple) SerializerAny.deserialize(bytes);
						//System.err.println(tuple);
						finalAcc.recommendations += tuple.recommendations;
						finalAcc.count += tuple.count;
						return SerializerAny.serialize(finalAcc);
					}
				}/*, Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))*/
	/*			.toStream()
				.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>>() {
					@Override
					public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
						FinalTuple tuple = (FinalTuple) SerializerAny.deserialize(bytes);
						CommentRank commentRank = new CommentRank(longWindowed.key(), tuple.recommendations * wa + tuple.count * wb);
						//System.err.println(commentRank);
						return new KeyValue<>(new Windowed<>(1L, longWindowed.window()), SerializerAny.serialize(commentRank));
					}
				});


		aggregateStream
				.groupByKey(Grouped.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.aggregate(new Initializer<byte[]>() {
					@Override
					public byte[] apply() {
						CommentRank[] comments = new CommentRank[]
								{new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0),
										new CommentRank(0L, 0.0)};
						System.err.println("aAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaaaaaaAAAAAAAAAAAAAAaaaaAAAAAAAAAAAA");
						return SerializerAny.serialize(comments);
					}
				}, new Aggregator<Windowed<Long>, byte[], byte[]>() {
					@Override
					public byte[] apply(Windowed<Long> aLong, byte[] bytes, byte[] agg) {
						CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(agg);
						CommentRank comment = (CommentRank) SerializerAny.deserialize(bytes);

						for (int i = 0; i < 10; i++) {

							if (comments[i] == null) {
								comments[i] = comment;
								break;
							} else {
								if (comments[i].score <= comment.score) {
									for (int j = 8; j >= i; j--) {
										comments[j + 1] = comments[j];
									}
									comments[i] = comment;
									break;
								}
							}
						}
						return SerializerAny.serialize(comments);
					}
				}, Materialized.with(WindowedSerdes.timeWindowedSerdeFrom(Long.class, Duration.ofDays(1).toMillis()), Serdes.ByteArray()))
				.toStream()
				.map(new KeyValueMapper<Windowed<Long>, byte[], KeyValue<Long, String>>() {
					@Override
					public KeyValue<Long, String> apply(Windowed<Long> key, byte[] bytes) {

						CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(bytes);
						ObjectMapper mapper1 = new ObjectMapper();
						try {
							String value = mapper1.writeValueAsString(comments);
							System.err.println(Instant.ofEpochMilli(key.window().start()).atZone(ZoneId.of("UTC")).toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
							System.err.println(key.key() + " " + value);
							return new KeyValue<>(key.key(), value);
						} catch (JsonProcessingException e) {
							e.printStackTrace();
						}
						return new KeyValue<>(key.key(), "error");
					}
				}).to("results", Produced.with(Serdes.Long(), Serdes.String()));


				/*.foreach((k, v) -> {
			System.err.println(k.window().start() + " - " + k.window().end());
			CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(v);
			System.err.println("------------------------------------------------------------------------------");
			for (int i = 0; i < comments.length; i++) {
				System.err.println(i + " " + comments[i].userId + " - " + comments[i].score);
			}
			System.err.println("------------------------------------------------------------------------------");

		});*/

	}

}
