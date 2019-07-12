package serdes;

import com.fasterxml.jackson.databind.JsonNode;
import entity.Comment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * A timestamp extractor implementation that tries to extract event time from
 * the "timestamp" field in the Json formatted message.
 */
public class JsonTimestampExtractor implements TimestampExtractor {

	@Override
	public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {

		if (record.value() instanceof Comment) {
			if(((Comment) record.value()).getCreateDate() == null) return previousTimestamp;
			else return Long.valueOf(((Comment) record.value()).getCreateDate()) * 1000;
		}
		throw new IllegalArgumentException("JsonTimestampExtractor cannot recognize the record value " + record.value());
	}
}