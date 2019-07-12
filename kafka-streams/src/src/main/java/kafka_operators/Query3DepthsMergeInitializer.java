package kafka_operators;

import entity.CommentTupleByUser;
import org.apache.kafka.streams.kstream.Initializer;
import utils.SerializerAny;

public class Query3DepthsMergeInitializer implements Initializer<byte[]> {
	@Override
	public byte[] apply() {
		return SerializerAny.serialize(new CommentTupleByUser(0L, 0.0, 0.0, 0L));
	}
}
