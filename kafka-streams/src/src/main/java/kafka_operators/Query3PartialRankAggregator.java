package kafka_operators;

import entity.CommentRank;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3PartialRankAggregator implements Aggregator<Long, byte[], byte[]> {
	//incrementally add to the sorted partial rank the incoming value
	@Override
	public byte[] apply(Long aLong, byte[] bytes, byte[] agg) {
		CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(agg);
		CommentRank comment = (CommentRank) SerializerAny.deserialize(bytes);

		for (int i = 0; i < 10; i++) {

			if (comments[i] == null) {
				comments[i] = comment;
				break;
			} else {
				if (comments[i].score <= comment.score) {
					for (int j = 8; j >= i; j--) {
						comments[j + 1] = comments[j];
					}
					comments[i] = comment;
					break;
				}
			}
		}
		return SerializerAny.serialize(comments);
	}}
