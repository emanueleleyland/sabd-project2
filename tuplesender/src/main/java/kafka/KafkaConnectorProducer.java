package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Kafka producer wrapper class. It is used to create a connection to a given kafka broker and to create
 * a serializer for the produced data
 */
public class KafkaConnectorProducer<T, S>{

    private Producer<T, S> producer;

    public KafkaConnectorProducer(String server) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                server);
        //configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        producer = new KafkaProducer(configProperties);
    }

    public void send(String topic, S tuple){
        producer.send(new ProducerRecord<T, S>(topic, tuple));
    }
}
