package kafka_operators;

import entity.CommentRank;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3GlobalRankAggregator implements Aggregator<Windowed<Long>, byte[], byte[]> {
	//incrementally add to the sorted global rank the incoming value
	@Override
	public byte[] apply(Windowed<Long> aLong, byte[] bytes, byte[] agg) {
		CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(agg);
		CommentRank[] partialComments = (CommentRank[]) SerializerAny.deserialize(bytes);
		CommentRank comment = null;
		for (int k = 0; k < 10; k++) {
			if (partialComments[k] != null) {
				comment = partialComments[k];
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
			}
		}
		return SerializerAny.serialize(comments);
	}
}
