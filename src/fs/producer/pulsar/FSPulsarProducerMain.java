package fs.producer.pulsar;

import java.util.Map;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class FSPulsarProducerMain {

	private static final String MESSAGE_SIZE_CONFIG = "MESSAGE_SIZE";

	private static final String RATE_LIMIT_CONFIG = "RATE_LIMIT";
	
	private static final String SERVICE_URL_CONFIG = "SERVICE_URL";
	
	private static final String TOPIC_NAME_CONFIG = "TOPIC_NAMES";
	
	public static void main(String[] args) throws PulsarClientException {

		int messageSizeInKB;
		Map<String, String> envMap = System.getenv();
		if (envMap.containsKey(MESSAGE_SIZE_CONFIG)) {
			messageSizeInKB = Integer.parseInt(envMap.get(MESSAGE_SIZE_CONFIG));
		} else {
			System.out.println("Warning - MESSAGE_SIZE missing from environment, defaulting to 750KB");
			messageSizeInKB = 750;
		}

		int upperRateLimitInKB;
		if (envMap.containsKey(RATE_LIMIT_CONFIG)) {
			upperRateLimitInKB = Integer.parseInt(envMap.get(RATE_LIMIT_CONFIG));
		} else {
			System.out.format("Warning - RATE_LIMIT missing from environment, defaulting to %d KB/s.%n", 75000);
			upperRateLimitInKB = 75000;
		}
		
		String serviceUrl;
		if (envMap.containsKey(SERVICE_URL_CONFIG)) {
			serviceUrl = envMap.get(SERVICE_URL_CONFIG);
		} else {
			System.out.format("Warning - BOOTSTRAP_SERVERS missing from environment, defaulting to localhost:9092");
			serviceUrl = "localhost:9092";
		}
		
		String topicName;
		if (envMap.containsKey(TOPIC_NAME_CONFIG)) {
			topicName = envMap.get(TOPIC_NAME_CONFIG);
		} else {
			System.out.format("Warning - TOPIC_NAME missing from environment, defaulting to sensor0");
			topicName = "sensor0";
		}

		PulsarClient client = PulsarClient.builder()
		        .serviceUrl(serviceUrl)
		        .build();

		FSPulsarThrottledProducer fsProducer = new FSPulsarThrottledProducer(client, topicName, messageSizeInKB, upperRateLimitInKB);
		Thread thread = new Thread(fsProducer);
		thread.start();
	}
}
