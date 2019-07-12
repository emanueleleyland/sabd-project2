package kafka_streams;

public class KafkaConfig {

	//Kafka broker hostname
	//public static String KAFKA_ADDR = "localhost";
	public static String KAFKA_ADDR = "kafka.cini-project.cloud";

	//kafka broker port number
	public static String KAFKA_PORT = "9092";

	//Kafka application parallelism used to generate a random value
	//to distribute the computation over different task managers
	public static Integer PARALLELISM = 2;

	//The input topic where the dataset is produced
	public static String INPUT_TOPIC = "comments6";

	//Pseudo-random number generator seed
	public static Long SEED = 1234L;

}
