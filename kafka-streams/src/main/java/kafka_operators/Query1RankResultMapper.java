package kafka_operators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import entity.RankRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import utils.SerializerAny;

public class Query1RankResultMapper implements KeyValueMapper<Windowed<Long>, byte[], KeyValue<Long, String>> {

	@Override
	public KeyValue<Long, String> apply(Windowed<Long> longWindowed, byte[] bytes) {
		ObjectMapper mapper = new ObjectMapper();
		RankRecord[] records = (RankRecord[]) SerializerAny.deserialize(bytes);
		String res = "";
		try {
			res = mapper.writeValueAsString(records);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		return new KeyValue<>(longWindowed.key(), res);
	}

}
