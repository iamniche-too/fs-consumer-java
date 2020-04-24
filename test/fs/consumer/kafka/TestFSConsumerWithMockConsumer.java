package fs.consumer.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;

public class TestFSConsumerWithMockConsumer {

	public FSConsumer<String, byte[]> fsConsumer;

	@Test
	public void testWithMockSendingMessages() {
		List<String> topicNames = new ArrayList<String>();
		topicNames.add("sensor1");

		Consumer<String, byte[]> mockConsumer = new MockConsumer(true);
		fsConsumer = new FSConsumer(mockConsumer, topicNames);
		
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
		
		assertTrue(fsConsumer.getTotalKbsTransferred() > 0);
	}
	
	@Test
	public void testWithMockNoMessages() {
		List<String> topicNames = new ArrayList<String>();
		topicNames.add("sensor1");

		Consumer<String, byte[]> mockConsumer = new MockConsumer(false);
		fsConsumer = new FSConsumer(mockConsumer, topicNames);
		
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
		
		assertTrue(fsConsumer.getTotalKbsTransferred() > 0);
	}
}
