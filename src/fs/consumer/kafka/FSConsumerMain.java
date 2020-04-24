package fs.consumer.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
 * 
 */
public class FSConsumerMain {

	private static final String CONSUMER_GROUP_ID_CONFIG = "CONSUMER_GROUP_ID";
	private static final String BOOTSTRAP_SERVERS_CONFIG = "BOOTSTRAP_SERVERS";
	private static final String TOPIC_NAMES_CONFIG = "TOPIC_NAMES";
	private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
	
	private static String generateConsumerGroupId() {
		String consumerGroupId = UUID.randomUUID().toString();
		return consumerGroupId;
	}
	
	public static void main(String[] args) {
		String consumerGroupId;
		Map<String, String> envMap = System.getenv();
		if (envMap.containsKey(CONSUMER_GROUP_ID_CONFIG)) {
			consumerGroupId = envMap.get(CONSUMER_GROUP_ID_CONFIG);
		} else {
			
			consumerGroupId = generateConsumerGroupId();
			System.out.format("Warning - CONSUMER_GROUP_ID defaulting to %s.%n", consumerGroupId);
		}

		String bootstrapServers;
		if (envMap.containsKey(BOOTSTRAP_SERVERS_CONFIG)) {
			bootstrapServers = envMap.get(BOOTSTRAP_SERVERS_CONFIG);
		} else {
			// production = internal-service-0.kafka.svc.cluster.local:32400
			
			bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
			System.out.format("Warning - bootstrap.servers defaulting to =%s%n", bootstrapServers);
		}
		
		List<String> topicNames = new ArrayList<String>();
		if (envMap.containsKey(TOPIC_NAMES_CONFIG)) {
			String configTopicNames = envMap.get(TOPIC_NAMES_CONFIG);
			topicNames.addAll(Arrays.asList(configTopicNames.split(",")));
		} else {
			for (int i = 1; i <= 50; i++) {
				topicNames.add("sensor" + i);
			}
			System.out.format("Warning - TOPIC_NAMES missing from environment, defaulting to %s", topicNames.toArray());	
		}
		
		Properties kafkaConsumerConfig = new Properties();
		kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		// config.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
		kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		// Whether to only listen for messages that occurred since the consumer started
		// ('latest'),
		// or to pick up all messages that the consumer has missed ('earliest').
		// Using 'latest' means the consumer must be started before the producer.
		kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// Required for Java client
		kafkaConsumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		System.out.format("Connecting to Kafka Server on %s%n", DEFAULT_BOOTSTRAP_SERVERS);
		KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<Integer, String>(kafkaConsumerConfig);
		FSConsumer<Integer, String> fsConsumer = new FSConsumer<Integer, String>(kafkaConsumer, topicNames);

		Thread thread = new Thread(fsConsumer);
		thread.start();
	}
}
