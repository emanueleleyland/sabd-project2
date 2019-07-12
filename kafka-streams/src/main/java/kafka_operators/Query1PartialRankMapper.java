package kafka_operators;

import entity.RankRecord;
import kafka_streams.KafkaConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

import java.util.Random;

public class Query1PartialRankMapper implements KeyValueMapper<Windowed<String>, Long, KeyValue<Long, byte[]>> {

	Random random = new Random(KafkaConfig.SEED);

	//emit the result of aggregation prepending to the tuple a random number
	//between 1 and # of task manager to allow a distributed sorting of the ranking
	@Override
	public KeyValue<Long, byte[]> apply(Windowed<String> k, Long v) {
		return new KeyValue<Long, byte[]>((long) random.nextInt(KafkaConfig.PARALLELISM), SerializerAny.serialize(new RankRecord(k.key(), v, k.window().start())));
	}

}
