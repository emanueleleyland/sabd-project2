package kafka_operators;

import entity.CommentTupleByUser;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3Depth1And2MergeMapper implements KeyValueMapper<Long, byte[], KeyValue<Long, byte[]>> {
	@Override
	public KeyValue<Long, byte[]> apply(Long aLong, byte[] bytes) {
		CommentTupleByUser commentTupleByUser = null;
		if (bytes == null) return new KeyValue<>(null, null);
		commentTupleByUser = (CommentTupleByUser) SerializerAny.deserialize(bytes);
		if (commentTupleByUser.inReplyTo != 0L) {
			CommentTupleByUser c = new CommentTupleByUser(0L, commentTupleByUser.indirectCount, -1.0, commentTupleByUser.userId);
			return new KeyValue<>(commentTupleByUser.inReplyTo, SerializerAny.serialize(c));
		} else {
			CommentTupleByUser c = new CommentTupleByUser(0L, commentTupleByUser.indirectCount, commentTupleByUser.recommendations, commentTupleByUser.userId);
			return new KeyValue<>(aLong, SerializerAny.serialize(c));
		}
	}
}
