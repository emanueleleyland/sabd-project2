package kafka_operators;

import entity.FinalTuple;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3FinalMergeAggregator implements Aggregator<Windowed<Long>, byte[], byte[]> {
	@Override
	public byte[] apply(Windowed<Long> longWindowed, byte[] bytes, byte[] acc) {
		FinalTuple finalAcc = (FinalTuple) SerializerAny.deserialize(acc);
		FinalTuple tuple = (FinalTuple) SerializerAny.deserialize(bytes);
		finalAcc.recommendations += tuple.recommendations;
		finalAcc.count += tuple.count;
		return SerializerAny.serialize(finalAcc);
	}
}
