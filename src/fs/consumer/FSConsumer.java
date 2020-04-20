package fs.consumer;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.KafkaException;
import org.cache2k.benchmark.jmh.ForcedGcMemoryProfiler;

/**
 * Based on:
 * https://github.com/apache/kafka/blob/trunk/examples/src/main/java/kafka/examples/Consumer.java
 * 
 * @author nic
 *
 * @param Integer
 * @param String
 */
public class FSConsumer<K, V> implements Runnable {

	private static final String MESSAGE_TIMESTAMP = "MESSAGE_TIMESTAMP";

	private static final String MESSAGE_SIZE_KBS = "MESSAGE_SIZE_KBS";

	private static final String MESSAGE_TOPIC = "MESSAGE_TOPIC";

	public static final String ENDPOINT_URL_CONFIG = "ENDPOINT_URL";

	private Consumer<String, byte[]> consumer = null;;

	private AtomicBoolean shutdown;

	private CountDownLatch shutdownLatch;

	private long POLL_INTERVAL_IN_MS = 5 * 1000;

	private long THROUGHPUT_DEBUG_INTERVAL_SEC = 10;

	private int NO_MESSAGE_REPORTING_INTERVAL_IN_MILLIS = 10 * 1000;

	private int KBS_IN_MB = 1000;

	private int noMessageCount = 0;

	private long currentTime, noMessageReportTime;

	private long kBsInWindow, totalKBs = 0;

	private long windowStartTime = 0;

	private Properties properties;

	private long initialMemoryUsageInBytes = 0;
	
	private long peakMemoryUsageInBytes = 0;
	
	public FSConsumer(Consumer consumer, List<String> topics, Properties properties) {
		this.consumer = consumer;
		this.properties = properties;

		this.consumer.subscribe((Collection<java.lang.String>) topics);

		this.shutdown = new AtomicBoolean(false);
		this.shutdownLatch = new CountDownLatch(1);
	}

	private Map<String, Object> processRecord(ConsumerRecord record) {
		if (record == null)
			return null;

		Map<String, Object> meta = new HashMap<String, Object>();
		meta.put(MESSAGE_TOPIC, record.topic());
		meta.put(MESSAGE_SIZE_KBS, record.serializedValueSize() / 1000);
		meta.put(MESSAGE_TIMESTAMP, record.timestamp());
		return meta;
	}

	private void processMeta(Map<String, Object> meta) {
		// Maintain figures for throughput reporting
		int messageSize = (int) meta.get(MESSAGE_SIZE_KBS);

		this.kBsInWindow += messageSize;
		this.totalKBs += messageSize;
	}

	private void reportThroughput(long kBsInWindow, long windowLengthInSecs) {
		float throughputMBPerS = (float)(kBsInWindow / (float)(windowLengthInSecs * KBS_IN_MB));
		System.out.format("[FSConsumer] - Throughput in window (%d KB in %d secs): %.2f MB/s%n", kBsInWindow, windowLengthInSecs, throughputMBPerS);
		System.out.format("[FSConsumer] - Total transferred: %d MBs%n", totalKBs/1000);
	}

	private void reportPeakMemoryUse() {
		long settledMemoryInBytes = getSettledUsedMemory();
		
		System.out.println("[FSConsumer] (from ForcedGcMemoryProfiler) Heap + Non-heap post-GC memory usage: " + getSettledUsedMemory() + "");
		
		if (settledMemoryInBytes > peakMemoryUsageInBytes) {
			peakMemoryUsageInBytes = settledMemoryInBytes;
		}
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
				Thread.sleep(567);
			} catch (InterruptedException e) {}
			
			m = m2;
			m2 = ForcedGcMemoryProfiler.getUsedMemory();
		} while (m2 < getReallyUsedMemory());
		return m;
	}

	private void reportToEndpoint() {
		String endpointUrl = "print";

		if (this.properties.containsKey("ENDPOINT_URL")) {
			endpointUrl = (String) this.properties.get("ENDPOINT_URL");
		}

		if (endpointUrl.startsWith("http://")) {
			// TODO
		}
	}

	private void report(long kBsInWindow, long windowLengthInSecs) {
		reportThroughput(kBsInWindow, windowLengthInSecs);
		reportPeakMemoryUse();
		// reportToEndpoint();
	}

	private void processNoMessage() {
		noMessageCount++;

		if ((currentTime - noMessageReportTime) > NO_MESSAGE_REPORTING_INTERVAL_IN_MILLIS) {
			System.out.println("[FSConsumer] - Number of no messages: " + noMessageCount);
			// reset the no message count and report time
			noMessageCount = 0;
			noMessageReportTime = currentTime;
		}
	}

	public void run() {
		System.out.println("[FSConsumer] - Running...");

		// record initial memory use
		initialMemoryUsageInBytes = getSettledUsedMemory();
		
		try {
			noMessageReportTime = System.currentTimeMillis();
			windowStartTime = System.currentTimeMillis();

			while (!shutdown.get()) {
				try {
					// System.out.println("[FSConsumer] - Polling...");
					ConsumerRecords<String, byte[]> records = this.consumer
							.poll(Duration.ofMillis(POLL_INTERVAL_IN_MS));
					currentTime = System.currentTimeMillis();

					if (records != null) {
						if (records.isEmpty()) {
							processNoMessage();
						} else {
							for (ConsumerRecord<String, byte[]> record : records) {
								Map<String, Object> meta = processRecord(record);

								if (meta != null) {
									processMeta(meta);
								} else {
									processNoMessage();
								}
							}
						}
					} else {
						processNoMessage();
					}

					// Determine if we should report throughput
					long windowLengthInSecs = (this.currentTime - this.windowStartTime) / 1000;

					if (windowLengthInSecs > THROUGHPUT_DEBUG_INTERVAL_SEC) {
						report(kBsInWindow, windowLengthInSecs);

						// Reset ready for the next throughput indication
						windowStartTime = System.currentTimeMillis();
						kBsInWindow = 0;
					}
				} catch (KafkaException e) {
					System.out.println("KafkaException from client: " + e.getMessage());
				}
			}
		} finally {
			this.consumer.close();
			shutdownLatch.countDown();
		}

		System.out.println("[FSConsumer] - Exiting...");
	}

	public void shutdown() throws InterruptedException {
		System.out.println("[FSConsumer] - Shutting down...");
		
		System.out.format("[FSConsumer] peakMemoryUsageInBytes=%d%n", peakMemoryUsageInBytes);
		System.out.format("[FSConsumer] memoryIncrease=%d%n", (peakMemoryUsageInBytes - initialMemoryUsageInBytes));
		
		shutdown.set(true);
		shutdownLatch.await();
	}

	public long getTotalKbs() {
		return this.totalKBs;
	}
}
