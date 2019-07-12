package kafka_operators;

import entity.CommentTuple;
import entity.CommentTupleByUser;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3Depth1And3MergeAggregator implements Aggregator<Windowed<Long>, byte[], byte[]> {
	@Override
	public byte[] apply(Windowed<Long> longWindowed, byte[] bytes, byte[] agg) {
		CommentTuple commentTuple = (CommentTuple) SerializerAny.deserialize(bytes);
		CommentTupleByUser commentTupleByUser = (CommentTupleByUser) SerializerAny.deserialize(agg);
		if (commentTuple.userId == 0L) {
			commentTupleByUser.indirectCount = commentTuple.count;
		} else {
			commentTupleByUser.userId = commentTuple.userId;
			commentTupleByUser.recommendations = commentTuple.recommendations;
		}
		return SerializerAny.serialize(commentTupleByUser);
	}
}
