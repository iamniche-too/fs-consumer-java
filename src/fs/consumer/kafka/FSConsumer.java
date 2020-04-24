package fs.consumer.kafka;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.cache2k.benchmark.jmh.ForcedGcMemoryProfiler;

public class FSConsumer<K, V> extends AConsumer {

	private long initialMemoryUsageInBytes = 0;

	private long peakMemoryUsageInBytes = 0;

	private long previousIncreaseInBytes = 0;
	
	private int reportMemoryCount = 1;
	
	public FSConsumer(Consumer consumer, List<String> topics) {
		super(consumer, topics);
		
		// record initial memory use
		initialMemoryUsageInBytes = getSettledUsedMemory();
	}

	private void reportPeakMemoryUse() {
		long settledMemoryInBytes = getSettledUsedMemory();

		if (settledMemoryInBytes > peakMemoryUsageInBytes) {
			peakMemoryUsageInBytes = settledMemoryInBytes;
		}
		
		long totalMemoryIncreaseInBytes = (peakMemoryUsageInBytes - initialMemoryUsageInBytes);
		
		System.out.format("[FSConsumer] - peakMemoryUsageInBytes=%d%n", peakMemoryUsageInBytes);
		System.out.format("[FSConsumer] - totalMemoryIncreaseInBytes=%d%n", totalMemoryIncreaseInBytes);
		System.out.format("[FSConsumer] - increaseFromPreviousReport=%d%n", (totalMemoryIncreaseInBytes - previousIncreaseInBytes));
		
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

	protected void report(long kBsInWindow) {
		super.report(kBsInWindow);
		
		// report as specified
		// the +5 means we report on the *first* time called ;)
		if ((reportMemoryCount+5) % 6 == 0) {
			reportPeakMemoryUse();
		}
		
		reportMemoryCount++;
	}
}
