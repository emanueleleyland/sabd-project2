package kafka_operators;

import entity.CommentTuple2;
import entity.IndirectComments2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3Depth2And3MergeMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>> {
	@Override
	public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
		IndirectComments2 indirectComments2 = null;
		Long indirectComments = null;
		CommentTuple2 commentTuple2 = new CommentTuple2(0L, 0L);
		try {
			indirectComments2 = (IndirectComments2) SerializerAny.deserialize(bytes);
			commentTuple2.inReplyTo = indirectComments2.inReplyTo;
			commentTuple2.userIdOrCount = indirectComments2.userId;
			//return new KeyValue<>(longWindowed, SerializerAny.serialize(new CommentTuple2(indirectComments2.userId, indirectComments2.inReplyTo)));
		} catch (ClassCastException e) {
			indirectComments = (Long) SerializerAny.deserialize(bytes);
			commentTuple2.inReplyTo = 0L;
			commentTuple2.userIdOrCount = indirectComments;
		}
		return new KeyValue<>(longWindowed, SerializerAny.serialize(commentTuple2));
	}
}
