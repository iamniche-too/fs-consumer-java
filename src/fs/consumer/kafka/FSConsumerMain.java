package fs.consumer.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
 * 
 */
public class FSConsumerMain {

	private static final String GCP_BOOTSTRAP_SERVERS = "internal-service-0.kafka.svc.cluster.local:32400";
	private static final String POD_NAME = "POD_NAME";
	private static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";
	
	public static void main(String[] args) {
		String consumerId;
		Map<String, String> envMap = System.getenv();
		if (envMap.containsKey(POD_NAME)) {
			consumerId = envMap.get(POD_NAME);
		} else {
			throw new RuntimeException("POD_NAME is missing from environment.");
		}

		String bootstrapServers;
		if (envMap.containsKey(BOOTSTRAP_SERVERS)) {
			bootstrapServers = envMap.get(BOOTSTRAP_SERVERS);
		} else {
			System.out.format("Defaulting to bootstrap.servers=%s%n", GCP_BOOTSTRAP_SERVERS);
			bootstrapServers = GCP_BOOTSTRAP_SERVERS;
		}
		
		Properties kafkaConsumerConfig = new Properties();
		kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		// config.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
		kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, consumerId);
		// Whether to only listen for messages that occurred since the consumer started
		// ('latest'),
		// or to pick up all messages that the consumer has missed ('earliest').
		// Using 'latest' means the consumer must be started before the producer.
		kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// Required for Java client
		kafkaConsumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		List<String> topicNames = new ArrayList<String>();
		for (int i = 1; i <= 50; i++) {
			topicNames.add("sensor" + i);
		}

		Properties fsConsumerConfig = new Properties();
		fsConsumerConfig.put(FSConsumer.ENDPOINT_URL_CONFIG, "http://");

		System.out.format("Connecting to Kafka Server on %s%n", GCP_BOOTSTRAP_SERVERS);
		KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<Integer, String>(kafkaConsumerConfig);
		FSConsumer<Integer, String> fsConsumer = new FSConsumer<Integer, String>(kafkaConsumer, topicNames,
				fsConsumerConfig);

		fsConsumer.run();
	}
}
