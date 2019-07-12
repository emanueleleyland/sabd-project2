package kafka_operators;

import entity.CommentTuple2;
import entity.CommentTupleByUser;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3Depth2And3MergeAggregator implements Aggregator<Windowed<Long>, byte[], byte[]> {

	@Override
	public byte[] apply(Windowed<Long> aLong, byte[] bytes, byte[] agg) {
		CommentTuple2 commentTuple = (CommentTuple2) SerializerAny.deserialize(bytes);
		CommentTupleByUser commentTupleByUser = (CommentTupleByUser) SerializerAny.deserialize(agg);
		if (commentTuple.inReplyTo == 0L) {
			commentTupleByUser.indirectCount = commentTuple.userIdOrCount.doubleValue();
		} else {
			commentTupleByUser.userId = commentTuple.userIdOrCount;
			commentTupleByUser.recommendations = 0.0;
			commentTupleByUser.inReplyTo = commentTuple.inReplyTo;
		}

		return SerializerAny.serialize(commentTupleByUser);
	}
}
