package serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import entity.Comment;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonPOJODeserializer<T> implements Deserializer<T> {
	private ObjectMapper objectMapper = new ObjectMapper();

	private Class<T> tClass;

	public JsonPOJODeserializer() {
	}

	@SuppressWarnings("unchecked")
	@Override
	public void configure(Map<String, ?> props, boolean isKey) {
		tClass = (Class<T>) props.get("JsonPOJOClass");
	}

	@Override
	public T deserialize(String topic, byte[] bytes) {
		if (bytes == null)
			return null;

		T data;
		try {
			data = objectMapper.readValue(bytes, tClass);
		} catch (Exception e) {
			throw new SerializationException(e);
		}

		if (tClass.equals(Comment.class)) {
			if (!((Comment) data).isValidComment()) ((Comment) data).setCreateDate(null);
		}

		return data;
	}

	@Override
	public void close() {

	}
}
