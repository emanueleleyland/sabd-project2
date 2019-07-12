package kafka_operators;

import entity.RankRecord;
import org.apache.kafka.streams.kstream.Aggregator;
import utils.SerializerAny;

public class Query1PartialRankAggregator implements Aggregator<Long, byte[], byte[]> {

	@Override
	public byte[] apply(Long aLong, byte[] value, byte[] agg) {
		RankRecord[] records = (RankRecord[]) SerializerAny.deserialize(agg);
		RankRecord record = (RankRecord) SerializerAny.deserialize(value);

		//check if new record has to be added in the partial rank. In affirmative
		//case shift other values in the rank

		for (int i = 0; i < 3; i++) {
			if (records[i] == null) {
				records[i] = record;
				break;
			} else {
				if (records[i].nComments <= record.nComments) {
					for (int j = 1; j >= i; j--) {
						records[j + 1] = records[j];
					}
					records[i] = record;
					break;
				}
			}
		}

		return SerializerAny.serialize(records);
	}

}
