package kafka_operators;

import entity.CommentRank;
import entity.FinalTuple;
import kafka_streams.KafkaConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

import java.util.Random;

public class Query3PartialRankMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Windowed<Long>, byte[]>> {
	Random random = new Random(KafkaConfig.SEED);
	private static Double wa = 0.3, wb = 0.7;

	@Override
	public KeyValue<Windowed<Long>, byte[]> apply(Windowed<Long> longWindowed, byte[] bytes) {
		//compute, for each user, his score based on recommendation and indirect count number
		FinalTuple tuple = (FinalTuple) SerializerAny.deserialize(bytes);
		CommentRank commentRank = new CommentRank(longWindowed.key(), tuple.recommendations * wa + tuple.count * wb);
		//prepend a random value that will be used as key to distribute the rank computation over nodes
		return new KeyValue<>(new Windowed<>((long) random.nextInt(KafkaConfig.PARALLELISM), longWindowed.window()), SerializerAny.serialize(commentRank));
	}
}
