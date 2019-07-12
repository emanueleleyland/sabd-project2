package kafka_operators;

import entity.RankRecord;
import org.apache.kafka.streams.kstream.Aggregator;
import utils.SerializerAny;

public class Query1GlobalRankAggregator implements Aggregator<Long, byte[], byte[]> {

	@Override
	public byte[] apply(Long aLong, byte[] value, byte[] agg) {
		RankRecord[] records = (RankRecord[]) SerializerAny.deserialize(agg);
		RankRecord[] patialRecords = (RankRecord[]) SerializerAny.deserialize(value);
		RankRecord record = null;

		//check if new record has to be added in the partial rank. In affirmative
		//case shift other values in the rank

		for (int k = 0; k < 3; k++) {
			if(patialRecords[k] != null) {
				record = patialRecords[k];
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
			}
		}
		return SerializerAny.serialize(records);
	}

}
