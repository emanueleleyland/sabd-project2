import com.fasterxml.jackson.core.JsonProcessingException;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import entity.Tuple;
import kafka.KafkaConnectorProducer;
import org.apache.kafka.common.protocol.types.Field;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class Main {

	//Kafka broker url
	private static final String KAFKA_URL = "kafka.cini-project.cloud:9092";
	//private static final String KAFKA_URL = "localhost:9092";

	//input topic to produce data
	private static final String KAFKA_TOPIC = "comments";

	//dataset csv file path
	private static final String FILE_PATH = "/Users/emanuele/Università/magistrale/quinto anno/SABD/Progetto2/Comments_jan-apr2018.csv";
	//private static final int maxTime = 7200;

	//Interval time to send all the dataset
	private static final int maxTime = 3600;

	//First tuple timestamp
	private static final double startTime = 1514767661.0;

	//Last tuple timestamp
	private static final double endTime = 1524085781.0;

	//Total dataset duration, in terms of event time
	private static final double delta = endTime - startTime;

	public static void main(String[] args) throws IOException, InterruptedException {
		//create a new connection to kafka broker
		KafkaConnectorProducer<String, String> producer = new KafkaConnectorProducer<String, String>(KAFKA_URL);
		//start to produce data
		realSender(producer);
	}

	private static void realSender(KafkaConnectorProducer<String, String> producer) throws FileNotFoundException, InterruptedException, JsonProcessingException {
		CsvToBean<Tuple> csvToBean = new CsvToBeanBuilder<Tuple>(new FileReader(FILE_PATH))
				.withType(Tuple.class).build();
		Iterator<Tuple> iterator = csvToBean.iterator();
		Tuple tuple = null;
		Long lastTime = 0L, time = null, sleepTime = null, tupleTime = null;
		String json;
		int day = 0;
		int i = 0;
		LocalDateTime localDateTime;
		LocalDateTime old = null;

		//read one csv file line at a time
		while (iterator.hasNext()) {
			tuple = iterator.next();
			tupleTime = Long.valueOf(tuple.getCreateDate());

			//time interval before sending next tuple
			time = (long) ((((tupleTime - startTime) / delta)) * maxTime);
			sleepTime = (time - lastTime) * 1000;

			//sleep for that time
			Thread.sleep(sleepTime);
			lastTime = time;

			json = tuple.toJson();
			localDateTime = Instant.ofEpochMilli(tupleTime * 1000).atZone(ZoneId.of("UTC")).toLocalDateTime();
			if(day < localDateTime.getDayOfYear()) {
				day = localDateTime.getDayOfYear();
				System.out.println(localDateTime);
			}

			//produce record on kafka
			producer.send(KAFKA_TOPIC, json);

		}

	}

}
