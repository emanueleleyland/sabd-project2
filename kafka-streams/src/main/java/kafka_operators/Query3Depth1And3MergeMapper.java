package kafka_operators;

import entity.CommentTuple;
import entity.DirectCommentTuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3Depth1And3MergeMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Long, byte[]>> {
	@Override
	public KeyValue<Long, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
		DirectCommentTuple directCommentTuple = null;
		Long indirectComments = null;
		CommentTuple commentTuple = new CommentTuple(0L, 0.0, 0.0);
		try {
			directCommentTuple = (DirectCommentTuple) SerializerAny.deserialize(bytes);
			commentTuple.userId = directCommentTuple.userId;
			commentTuple.recommendations = directCommentTuple.recommendations;

		} catch (ClassCastException e) {
			indirectComments = (Long) SerializerAny.deserialize(bytes);
			commentTuple.count = indirectComments.doubleValue();
		}

		return new KeyValue<>(longWindowed.key(), SerializerAny.serialize(commentTuple));	}
}
