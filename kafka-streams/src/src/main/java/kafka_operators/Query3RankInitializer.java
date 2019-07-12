package kafka_operators;

import entity.CommentRank;
import org.apache.kafka.streams.kstream.Initializer;
import utils.SerializerAny;

public class Query3RankInitializer implements Initializer<byte[]> {

	//zero-initialize a 10-size array to store the rank
	@Override
	public byte[] apply() {
		CommentRank[] comments = new CommentRank[]
				{new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0),
						new CommentRank(0L, 0.0)};
		return SerializerAny.serialize(comments);
	}
}
