package kafka_operators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.CommentRank;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query3FinalRankMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Long, String>> {

	@Override
	public KeyValue<Long, String> apply(Windowed<Long> key, byte[] bytes) {
		CommentRank[] comments = (CommentRank[]) SerializerAny.deserialize(bytes);
		ObjectMapper mapper = new ObjectMapper();
		String value = "";
		try {
			value = mapper.writeValueAsString(comments);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return new KeyValue<>(key.window().start(), value);
	}

}
