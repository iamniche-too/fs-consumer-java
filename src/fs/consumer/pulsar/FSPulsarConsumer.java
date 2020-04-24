package fs.consumer.pulsar;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.cache2k.benchmark.jmh.ForcedGcMemoryProfiler;

public class FSPulsarConsumer implements Runnable {

	protected static final String MESSAGE_TIMESTAMP = "MESSAGE_TIMESTAMP";

	protected static final String MESSAGE_SIZE_KB = "MESSAGE_SIZE_KB";

	protected static final String MESSAGE_TOPIC = "MESSAGE_TOPIC";

	private PulsarClient client;

	private Consumer consumer;

	private long initialMemoryUsageInBytes = 0;

	private long peakMemoryUsageInBytes = 0;

	private long previousIncreaseInBytes = 0;

	private int reportMemoryCount = 1;

	private long currentTime = 0;

	private long noMessageReportTime = 0;

	private int noMessageCount = 0;

	private int THROUGHPUT_DEBUG_INTERVAL_IN_MILLIS = 10 * 1000;

	private int NO_MESSAGE_REPORTING_INTERVAL_IN_MILLIS = 10 * 1000;

	private long kBsInWindow, totalKBsTransferred = 0;
	
	private long windowStartTime = 0;
	
	public FSPulsarConsumer(PulsarClient client, String topicName, String subscriptionName) throws PulsarClientException {
		this.client = client;
		consumer = client.newConsumer().topic(topicName).subscriptionType(SubscriptionType.Exclusive).subscriptionName(subscriptionName).subscribe();
	}

	private Map<String, Object> processMessage(Message message) {
		// extract metadata to map so ConsumerRecord doesn't "pollute" this codebase
		Map<String, Object> meta = new HashMap<String, Object>();
		meta.put(MESSAGE_TOPIC, message.getTopicName());

		int messageSizeKB = 0;
		byte[] data = message.getData();
		if (data != null) {
			messageSizeKB = data.length / 1000;
		}

		// System.out.format("[FSPulsarConsumer] - Received message of size %d KB.%n",
		// messageSizeKB);

		meta.put(MESSAGE_SIZE_KB, messageSizeKB);
		meta.put(MESSAGE_TIMESTAMP, message.getPublishTime());

		return meta;
	}

	// process the metadata from the message
	private void processMeta(Map<String, Object> meta) {
		// Maintain figures for throughput reporting
		int messageSizeInKB = (int) meta.get(MESSAGE_SIZE_KB);

		kBsInWindow += messageSizeInKB;
		totalKBsTransferred += messageSizeInKB;
	}

	private void processNoMessage() {
		noMessageCount++;

		if ((currentTime - noMessageReportTime) > NO_MESSAGE_REPORTING_INTERVAL_IN_MILLIS) {
			System.out.println("[FSPulsarConsumer] - Number of polls returning no messages: " + noMessageCount);
			// reset the no message count and report time
			noMessageCount = 0;
			noMessageReportTime = currentTime;
		}
	}

	public void run() {
		while (true) {

			Message message = null;
			try {
				// Receive message
				message = consumer.receive();

				currentTime = System.currentTimeMillis();

				if (message != null) {
					Map<String, Object> meta = processMessage(message);

					if (meta != null) {
						processMeta(meta);
					} else {
						processNoMessage();
					}
				} else {
					processNoMessage();
				}

				if ((currentTime - windowStartTime) > THROUGHPUT_DEBUG_INTERVAL_IN_MILLIS) {
					report(kBsInWindow);

					// Reset ready for the next throughput indication
					windowStartTime = System.currentTimeMillis();
					kBsInWindow = 0;
				}

				// Do something with the message
				System.out.printf("Message received: %s", new String(message.getData()));

				// Acknowledge the message so that it can be deleted by the message broker
				consumer.acknowledge(message);
			} catch (

			PulsarClientException e) {
				// Message failed to process, re-deliver later
				if (message != null) {
					consumer.negativeAcknowledge(message);
				}
			}
		}
	}

	private void reportThroughput(long kBsInWindow) {
		float throughputMBPerS = (float) (kBsInWindow / THROUGHPUT_DEBUG_INTERVAL_IN_MILLIS);
		System.out.format("[FSPulsarConsumer] - Throughput in window (%d KB in %d secs): %.2f MB/s%n", kBsInWindow,
				THROUGHPUT_DEBUG_INTERVAL_IN_MILLIS/1000, throughputMBPerS);
		System.out.format("[FSPulsarConsumer] - Total transferred: %d MBs%n", totalKBsTransferred / 1000);
	}
	
	public void report(long kBsInWindow) {
		reportThroughput(kBsInWindow);
		
		// report as specified
		// the +5 means we report on the *first* time called ;)
		if ((reportMemoryCount + 5) % 6 == 0) {
			reportPeakMemoryUse();
		}
		reportMemoryCount++;
	}

	private void reportPeakMemoryUse() {
		long settledMemoryInBytes = getSettledUsedMemory();

		if (settledMemoryInBytes > peakMemoryUsageInBytes) {
			peakMemoryUsageInBytes = settledMemoryInBytes;
		}

		long totalMemoryIncreaseInBytes = (peakMemoryUsageInBytes - initialMemoryUsageInBytes);

		System.out.format("[FSConsumer] - peakMemoryUsageInBytes=%d%n", peakMemoryUsageInBytes);
		System.out.format("[FSConsumer] - totalMemoryIncreaseInBytes=%d%n", totalMemoryIncreaseInBytes);
		System.out.format("[FSConsumer] - increaseFromPreviousReport=%d%n",
				(totalMemoryIncreaseInBytes - previousIncreaseInBytes));

		previousIncreaseInBytes = totalMemoryIncreaseInBytes;
	}

	private long getCurrentlyUsedMemory() {
		return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed()
				+ ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage().getUsed();
	}

	private long getGcCount() {
		long sum = 0;
		for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
			long count = b.getCollectionCount();
			if (count != -1) {
				sum += count;
			}
		}
		return sum;
	}

	private long getReallyUsedMemory() {
		long before = getGcCount();
		System.gc();
		while (getGcCount() == before)
			;
		return getCurrentlyUsedMemory();
	}

	private long getSettledUsedMemory() {
		long m;
		long m2 = getReallyUsedMemory();
		do {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}

			m = m2;
			m2 = ForcedGcMemoryProfiler.getUsedMemory();
		} while (m2 < getReallyUsedMemory());
		return m;
	}
}
