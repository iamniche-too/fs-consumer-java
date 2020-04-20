package fs.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestFSConsumerWithKafkaConsumer {

	public FSConsumer<String, byte[]> fsConsumer;

	public static String getConsumerId() {
		String consumerId = "";
		String idAlphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		Random random = new Random();
		for (int i = 0; i < 7; i++) {
			int index = random.nextInt(idAlphabet.length());
			consumerId = consumerId + idAlphabet.charAt(index);
		}
		return consumerId;
	}

	@BeforeEach
	public void setUp() {
		Properties kafkaConsumerConfig = new Properties();
		kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:32400");
		kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, getConsumerId());
		// Whether to only listen for messages that occurred since the consumer started
		// ('latest'),
		// or to pick up all messages that the consumer has missed ('earliest').
		// Using 'latest' means the consumer must be started before the producer.
		kafkaConsumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

		// Required fr Java client
		kafkaConsumerConfig.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		kafkaConsumerConfig.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
		
		List<String> topicNames = new ArrayList<String>();
		topicNames.add("sensor1");

		Properties fsConsumerConfig = new Properties();
		fsConsumerConfig.put(FSConsumer.ENDPOINT_URL_CONFIG, "http://");

		KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer(kafkaConsumerConfig);
		fsConsumer = new FSConsumer(kafkaConsumer, topicNames, fsConsumerConfig);
	}

	@Test
	public void testFSConsumer() {
		Thread thread = new Thread(fsConsumer);
		thread.start();
		
		// wait 60 seconds
		try {	
			Thread.sleep(60000);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		
		// wait for thread to exit
		try {	
			fsConsumer.shutdown();
			thread.join();
		} catch (InterruptedException ex) {
		}
		
		assertEquals(0, fsConsumer.getTotalKbs());
	}
}
