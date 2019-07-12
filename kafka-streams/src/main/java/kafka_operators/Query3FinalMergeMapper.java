package kafka_operators;

import entity.CommentTupleByUser;
import entity.FinalTuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3FinalMergeMapper implements KeyValueMapper<Long, byte[], KeyValue<Long, byte[]>> {
	@Override
	public KeyValue<Long, byte[]> apply(Long aLong, byte[] bytes) {
		if (bytes == null) return new KeyValue<>(null, null);
		CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(bytes);
		return new KeyValue<>(c.userId, SerializerAny.serialize(new FinalTuple(c.recommendations, c.indirectCount)));
	}
}
