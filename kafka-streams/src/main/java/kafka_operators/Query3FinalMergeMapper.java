package kafka_operators;

import entity.CommentTupleByUser;
import entity.FinalTuple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3FinalMergeMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>> {
	@Override
	public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> aLong, byte[] bytes) {
		if (bytes == null) return new KeyValue<>(null, null);
		CommentTupleByUser c = (CommentTupleByUser) SerializerAny.deserialize(bytes);
		return new KeyValue<>(new Windowed<>(c.userId, aLong.window()), SerializerAny.serialize(new FinalTuple(c.recommendations, c.indirectCount)));
	}
}
