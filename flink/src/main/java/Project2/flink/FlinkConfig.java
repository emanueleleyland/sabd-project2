package Project2.flink;

public class FlinkConfig {

    //Kafka broker hostname
    //public static String KAFKA_ADDR = "kafka.cini-project.cloud";
    public static String KAFKA_ADDR = "10.0.0.10";

    //kafka broker port number
    public static String KAFKA_PORT = "9092";

    //Flink application parallelism used to generate a random value
    //to distribute the computation over different task managers
    public static Integer PARALLELISM = 2;

    //The input topic where the dataset is produced
    public static String INPUT_TOPIC = "comments";

    //Pseudo-random number generator seed
    public static Long SEED = 1234L;
}
