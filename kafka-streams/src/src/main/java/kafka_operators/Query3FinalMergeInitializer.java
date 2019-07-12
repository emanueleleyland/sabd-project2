package kafka_operators;

import entity.FinalTuple;
import org.apache.kafka.streams.kstream.Initializer;
import utils.SerializerAny;

public class Query3FinalMergeInitializer implements Initializer<byte[]> {
	@Override
	public byte[] apply() {
		return SerializerAny.serialize(new FinalTuple(0.0, 0.0));
	}
}
