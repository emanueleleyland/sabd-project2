package kafka_operators;

import entity.RankRecord;
import org.apache.kafka.streams.kstream.Initializer;
import utils.SerializerAny;

public class Query1RankInitializer implements Initializer<byte[]>{
	@Override
	public byte[] apply() {
		//Emit empty rank with three fields
		RankRecord[] records = new RankRecord[]{null, null, null};

		return SerializerAny.serialize(records);
	}
}
