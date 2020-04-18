package org.cache2k.benchmark.jmh;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;

/**
 * Record the used heap memory of a benchmark iteration by forcing a full garbage collection.
 *
 * @author Jens Wilke
 */
public class ForcedGcMemoryProfiler {

  /**
   * Trigger a gc, wait for completion and return used memory. Inspired from JMH approach.
   *
   * <p>Before we had the approach of detecting the clearing of
   * a weak reference. Maybe this is not reliable, since when cleared the GC run may not
   * be finished.
   *
   * @see org.openjdk.jmh.runner.BaseRunner#runSystemGC()
   */
  public static long getUsedMemory() {
    final int MAX_WAIT_MSEC = 20 * 1000;
    List<GarbageCollectorMXBean> _enabledBeans = new ArrayList<GarbageCollectorMXBean>();
    for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
      long count = b.getCollectionCount();
      if (count != -1) {
        _enabledBeans.add(b);
      }
    }
    if (_enabledBeans.isEmpty()) {
      System.err.println("WARNING: MXBeans can not report GC info. Cannot extract reliable usage metric.");
      return -1;
    }
    long _beforeGcCount = countGc(_enabledBeans);
    long t0 = System.currentTimeMillis();
    System.gc();
    while (System.currentTimeMillis() - t0 < MAX_WAIT_MSEC) {
      try {
        Thread.sleep(234);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      long _gcCount;
      if ((_gcCount = countGc(_enabledBeans)) > _beforeGcCount) {
        MemoryUsage _heapUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        MemoryUsage _nonHeapUsage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        long totalMemory = _heapUsage.getCommitted() + _nonHeapUsage.getCommitted();
        long _usedHeapMemory = _heapUsage.getUsed();
        long _usedNonHeap = _nonHeapUsage.getUsed();
        //System.out.println("[ForcedGcMemoryProfiler] - getMemoryMXBean, usedHeap=" + _usedHeapMemory + ", usedNonHeap=" + _usedNonHeap + ", totalUsed=" + (_usedHeapMemory + _usedNonHeap) + ", gcCount=" + _gcCount);
        //System.out.println("[ForcedGcMemoryProfiler] - Runtime totalMemory-freeMemory, used memory: " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        return _usedHeapMemory + _usedNonHeap;
      }
    }
    System.err.println("WARNING: System.gc() was invoked but couldn't detect a GC occurring, is System.gc() disabled?");
    return -1;
  }

  private static long countGc(final List<GarbageCollectorMXBean> _enabledBeans) {
    long cnt = 0;
    for (GarbageCollectorMXBean bean : _enabledBeans) {
      cnt += bean.getCollectionCount();
    }
    return cnt;
  }

}
