package Project2.flink;

public class RedisConfig {

	// redis server address hostname
	public static final String REDIS_ADDR = "localhost";
	//redis server port
	public static final String REDIS_PORT = "6379";

	//expiration time for the record in the redis database
	public static int expirationTime = 360;
}
