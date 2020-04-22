package fs.consumer.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fs.consumer.kafka.FSConsumer;

public class TestFSConsumerWithMockConsumer {

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
	public void setUp() throws NoSuchAlgorithmException {		
		List<String> topicNames = new ArrayList<String>();
		topicNames.add("sensor1");

		Properties fsConsumerConfig = new Properties();
		fsConsumerConfig.put(FSConsumer.ENDPOINT_URL_CONFIG, "http://");

		Consumer<String, byte[]> mockConsumer = new MockConsumer();
		fsConsumer = new FSConsumer(mockConsumer, topicNames, fsConsumerConfig);
	}

	@Test
	public void testFSConsumer() {
		Thread thread = new Thread(fsConsumer);
		thread.start();
		
		// run for specific period
		try {	
			Thread.sleep(1000 * 60 * 5);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		
		// wait for thread to exit
		try {	
			fsConsumer.shutdown();
			thread.join();
		} catch (InterruptedException ex) {
		}
		
		assertTrue(fsConsumer.getTotalKbs() > 0);
	}
}
