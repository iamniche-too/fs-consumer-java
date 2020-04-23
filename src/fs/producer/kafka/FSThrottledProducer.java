package fs.producer.kafka;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.cache2k.benchmark.jmh.ForcedGcMemoryProfiler;

public class FSThrottledProducer implements Runnable {

	private int messageSizeInKB;

	private KafkaProducer producer;

	private AtomicBoolean shutdown;

	private final CountDownLatch shutdownLatch;

	private String topic;

	private int messageKey = 0;

	private boolean rateIsExceeded = false;

	private int rateCurrentSecond = 0;

	private int rateForSecondSoFar = 0;

	private int upperDataRateLimitKB;

	private int messagesSentInCurrentWindow = 0;

	private long windowStartTimeInMillis;

	private int throughputDebugIntervalInSec = 10;

	private static int KBs_IN_MB = 1000;

	private int maxPayloadsBeforeFlush = 5;

	private long totalTransferredInKB;

	private byte[] payload = null;
	
	private long initialMemoryUsageInBytes = 0;

	private long peakMemoryUsageInBytes = 0;

	private long previousIncreaseInBytes = 0;
	
	private int reportMemoryCount = 1;
	
	private boolean isReportMemory = false;
	
	public FSThrottledProducer(KafkaProducer producer, String topic, int messageSizeInKB, int upperDataRateLimitKB) {
		this.producer = producer;
		this.messageSizeInKB = messageSizeInKB;
		this.topic = topic;
		this.upperDataRateLimitKB = upperDataRateLimitKB;
		shutdown = new AtomicBoolean(false);
		shutdownLatch = new CountDownLatch(1);
		
		// record initial memory use
		initialMemoryUsageInBytes = getSettledUsedMemory();
	}

	private long getSettledUsedMemory() {
		long m;
		long m2 = getReallyUsedMemory();
		do {
			try {
				// wait for 100ms
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}

			m = m2;
			m2 = ForcedGcMemoryProfiler.getUsedMemory();
		} while (m2 < getReallyUsedMemory());
		return m;
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
	
	private void reportPeakMemoryUse() {
		long settledMemoryInBytes = getSettledUsedMemory();

		if (settledMemoryInBytes > peakMemoryUsageInBytes) {
			peakMemoryUsageInBytes = settledMemoryInBytes;
		}

		long memoryIncreaseInBytes = (peakMemoryUsageInBytes - initialMemoryUsageInBytes);
		
		System.out.format("[FSThrottledProducer] - peakMemoryUsageInBytes=%d%n", peakMemoryUsageInBytes);
		System.out.format("[FSThrottledProducer] - memoryIncreaseInBytes=%d%n", memoryIncreaseInBytes);
		System.out.format("[FSThrottledProducer] - delta=%d%n", (memoryIncreaseInBytes - previousIncreaseInBytes));
		
		previousIncreaseInBytes = memoryIncreaseInBytes;
	}
	
	private byte[] getPayload() {
		
		if (payload == null) {
			System.out.println("[FSThrottledProducer] - generating payload...");
			long before = System.currentTimeMillis();
			payload = new byte[messageSizeInKB * 1000];
			new Random().nextBytes(payload);
			long after = System.currentTimeMillis();
			long differenceInSecs = (after - before) / 1000;
			System.out.println("[FSThrottledProducer] - payload generated in " + differenceInSecs + " secs.");
		}
		
		return payload;
	}

	private int getMessageKey() {
		return messageKey++;
	}

	public void payloadReceived(int payloadSizeInKB) {
		//System.out.format("[FSThrottledProducer] - ACK received for %d KBs.%n", payloadSizeInKB);

		long currentTimeInMillis = System.currentTimeMillis();

		// are we in the same second?
		if ((int) (currentTimeInMillis / 1000) != rateCurrentSecond) {
			// We are in a new second, we can reset rate throttling
			rateIsExceeded = false;
			rateForSecondSoFar = 0;
			rateCurrentSecond = (int) (currentTimeInMillis / 1000);
		}

		// Add the payload we've sent to the total so far
		rateForSecondSoFar += payloadSizeInKB;
		totalTransferredInKB += payloadSizeInKB;

		// Check if we've exceeded the upper limit of the rate
		if (rateForSecondSoFar >= upperDataRateLimitKB) {
			rateIsExceeded = true;
		}

		// Output any throughput debug
		messagesSentInCurrentWindow += 1;

		int windowLengthSec = (int) ((currentTimeInMillis - windowStartTimeInMillis) / 1000);

		if (windowLengthSec >= throughputDebugIntervalInSec) {
			int throughput = (messagesSentInCurrentWindow * payloadSizeInKB)
					/ (throughputDebugIntervalInSec * KBs_IN_MB);
			System.out.format("[FSThrottledProducer] - Throughput in window: %d MB/s.%n", throughput);
			
			if (isReportMemory) {
				// only report memory use every 10 windows
				if (reportMemoryCount % 10 == 0) {
					reportPeakMemoryUse();
					reportMemoryCount = 1;
				}
				
				reportMemoryCount++;
			}
			
			// Reset ready for the next throughput indication
			windowStartTimeInMillis = System.currentTimeMillis();
			messagesSentInCurrentWindow = 0;
		}
	}

	private void throttle() {
		// Check for rate being exceeded
		// i.e. we are sending data > upper rate limit
		while (rateIsExceeded) {
			//System.out.println("[FSThrottledProducer - rate exceeded, throttling...");
			
			// sleep for 10ms
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}

			// Check the current second
			long currentTimeInMillis = System.currentTimeMillis();

			// Remove rate limiting if we are in a new second
			if ((int) (currentTimeInMillis / 1000) != rateCurrentSecond) {
				rateIsExceeded = false;
				rateForSecondSoFar = 0;
			}
		}
	}

	private void flushMessages(int messageCount) {
		//Flush periodically so we can manage data rates effectively.
		//Produce is async so can get way ahead of writes actually being ack'd by Kafka.  If we don't do this
		//then we can't manage the data rate effectively.
		//The flush rate may need tuning.
		if ((messageCount+1) % maxPayloadsBeforeFlush == 0) {
			//System.out.println("[FSProducer]- Flushing sent messages.");
		    producer.flush();
		}
	}

	public void run() {
		System.out.format("[FSThrottledProducer] - Configured with message size %sKB, date rate %s KB/s %n", messageSizeInKB,
				upperDataRateLimitKB);

		windowStartTimeInMillis = System.currentTimeMillis();

		int messageCount = 0;
		while (!shutdown.get()) {
			throttle();
			
			long now = System.currentTimeMillis();
			byte[] payload = getPayload();
			int messageKey = getMessageKey();
			ProducerRecord producerRecord = new ProducerRecord(topic, messageKey, payload);
			Callback callback = new FSCallBack(this, now, messageKey, payload.length);
			
			// async call to send
			producer.send(producerRecord, callback);

			//System.out.println("[FSThrottledProducer] - sent message.");
			
			flushMessages(messageCount);
			messageCount++;
		}

		producer.close();
		shutdownLatch.countDown();

		System.out.println("[FSProducer] - Exiting...");
	}

	public void shutdown() throws InterruptedException {
		System.out.println("[FSProducer] - Shutting down...");
		System.out.format("[FSProducer] - Total bytes transferred=%d KB.%n", totalTransferredInKB);
		shutdown.set(true);
		shutdownLatch.await();
	}
}

class FSCallBack implements Callback {

	private final long startTime;
	private final int key;
	private final int messageSizeInBytes;

	private FSThrottledProducer producer;

	public FSCallBack(FSThrottledProducer producer, long startTime, int key, int messageSizeInBytes) {
		this.producer = producer;
		this.startTime = startTime;
		this.key = key;
		this.messageSizeInBytes = messageSizeInBytes;
	}

	/**
	 * A callback method the user can implement to provide asynchronous handling of
	 * request completion. This method will be called when the record sent to the
	 * server has been acknowledged. When exception is not null in the callback,
	 * metadata will contain the special -1 value for all fields except for
	 * topicPartition, which will be valid.
	 *
	 * @param metadata  The metadata for the record that was sent (i.e. the
	 *                  partition and offset). An empty metadata with -1 value for
	 *                  all fields except for topicPartition will be returned if an
	 *                  error occurred.
	 * @param exception The exception thrown during processing of this record. Null
	 *                  if no error occurred.
	 */
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			// Note - report in KB
			producer.payloadReceived(messageSizeInBytes/1000);

			//System.out.println("message(key=" + key + ", size=" + messageSize + ") sent to partition("
			//		+ metadata.partition() + "), " + "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else {
			exception.printStackTrace();
		}
	}
	
}
