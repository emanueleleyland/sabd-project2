package kafka_operators;

import entity.CommentTupleByUser;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3Depth1And2MergeAggregator implements Aggregator<Windowed<Long>, byte[], byte[]> {
	@Override
	public byte[] apply(Windowed<Long> longWindowed, byte[] bytes, byte[] agg) {
		CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(agg);
		CommentTupleByUser val = (CommentTupleByUser) SerializerAny.deserialize(bytes);

		c.userId = val.userId;

		if (val.recommendations != -1.0) {
			c.recommendations += val.recommendations;
		} else {

			c.indirectCount += val.indirectCount;
		}
		return SerializerAny.serialize(c);
	}
}
