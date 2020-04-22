package fs.producer.kafka;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class FSProducerMain {

	private static final String MESSAGE_SIZE = "MESSAGE_SIZE";

	private static final String RATE_LIMIT = "RATE_LIMIT";
	
	public static void main(String[] args) {

		int messageSizeInKB;
		Map<String, String> envMap = System.getenv();
		if (envMap.containsKey(MESSAGE_SIZE)) {
			messageSizeInKB = Integer.parseInt(envMap.get(MESSAGE_SIZE));
		} else {
			System.out.println("Warning - MESSAGE_SIZE missing from environment, defaulting to 750KB");
			messageSizeInKB = 750;
		}

		int upperRateLimitInKB;
		if (envMap.containsKey(RATE_LIMIT)) {
			upperRateLimitInKB = Integer.parseInt(envMap.get(RATE_LIMIT));
		} else {
			System.out.format("Warning - RATE_LIMIT missing from environment, defaulting to %d KB/s.%n", 75000);
			upperRateLimitInKB = 75000;
		}
		
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "FSThrottledProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		KafkaProducer producer = new KafkaProducer<>(props);

		String topic = "test";
		FSThrottledProducer fsProducer = new FSThrottledProducer(producer, topic, messageSizeInKB, upperRateLimitInKB);
		Thread thread = new Thread(fsProducer);
		thread.start();
	}
}
