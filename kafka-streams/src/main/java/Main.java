import entity.Comment;
import kafka_streams.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import serdes.JsonPOJODeserializer;
import serdes.JsonPOJOSerializer;
import serdes.JsonTimestampExtractor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

public class Main {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "SABD-project-22222");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.KAFKA_ADDR + ":" + KafkaConfig.KAFKA_PORT);
		props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
		//props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
		//props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "a");

		//metrics only
		props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG");
		props.put(StreamsConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, "3000");


		//props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		//props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder builder = new StreamsBuilder();


		Map<String, Object> serdeProps = new HashMap<>();

		Deserializer<Comment> commentDeserializer = new JsonPOJODeserializer<>();
		serdeProps.put("JsonPOJOClass", Comment.class);
		commentDeserializer.configure(serdeProps, false);

		Serializer<Comment> commentSerializer = new JsonPOJOSerializer<>();
		serdeProps.put("JsonPOJOClass", Comment.class);
		commentSerializer.configure(serdeProps, false);

		Serde<Comment> commentSerde = Serdes.serdeFrom(commentSerializer, commentDeserializer);


		KStream<String, Comment> kStream = builder
				.stream(KafkaConfig.INPUT_TOPIC, Consumed.with(Serdes.String(), commentSerde, new JsonTimestampExtractor(), Topology.AutoOffsetReset.LATEST))
				.filter((k, v) -> (v.getCreateDate() != null));

		Query1.doQuery(kStream);
		Query2.doQuery(kStream);
		Query3.doQuery(kStream);


		final Topology topology = builder.build();
		System.err.println(topology.describe());
		final KafkaStreams streams = new KafkaStreams(topology, props);
		streams.cleanUp();
		final CountDownLatch latch = new CountDownLatch(1);


		Map<MetricName, ? extends Metric> a = streams.metrics();
		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}

}
