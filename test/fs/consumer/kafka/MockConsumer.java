package fs.consumer.kafka;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

public class MockConsumer<K, V> implements Consumer<K, V> {

	private String topic = "topic";

	private int partition = 0;

	private long offset = 0;

	private String KEY = "Key";

	public MockConsumer() {
	}

	@Override
	public Set<TopicPartition> assignment() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> subscription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void subscribe(Collection<String> topics) {
		System.out.println("[MockConsumer] - subscribe called.");
	}

	@Override
	public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public void assign(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

	}

	@Override
	public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public void subscribe(Pattern pattern) {
		// TODO Auto-generated method stub

	}

	@Override
	public void unsubscribe() {
		// TODO Auto-generated method stub

	}

	@Override
	public ConsumerRecords<K, V> poll(long timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	private byte[] generatePayload() {
		//System.out.println("[MockConsumer] - generating payload...");
		//long before = System.currentTimeMillis();
		byte[] payload = new byte[750000];
		new Random().nextBytes(payload);
		//long after = System.currentTimeMillis();
		//long differenceInSecs = (after - before) / 1000;
		//System.out.println("[MockConsumer] - payload generated in " + differenceInSecs + " secs.");
		return payload;
	}
	
	@Override
	public ConsumerRecords<K, V> poll(Duration timeout) {
		
		Map<TopicPartition, List<ConsumerRecord<K, V>>> map = new HashMap<TopicPartition, List<ConsumerRecord<K, V>>>();
		
		List<ConsumerRecord<K, V>> list = new ArrayList<ConsumerRecord<K, V>>();
		
		long timestamp = System.currentTimeMillis();
		long checksum = 0;
		byte[] payload = generatePayload();
		//System.out.println("[MockConsumer] - payload is " + payload.length + " bytes");
		ConsumerRecord<K, V> record = new ConsumerRecord<K, V>(topic, partition, offset, timestamp, TimestampType.LOG_APPEND_TIME, checksum, 0, payload.length, (K)KEY, (V)payload);
		list.add(record);
		
		map.put(new TopicPartition(topic, 1), list);
		
		ConsumerRecords<K, V> records = new ConsumerRecords<K, V>(map);
		return records;
	}

	@Override
	public void commitSync() {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitSync(Duration timeout) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitAsync() {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitAsync(OffsetCommitCallback callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
		// TODO Auto-generated method stub

	}

	@Override
	public void seek(TopicPartition partition, long offset) {
		// TODO Auto-generated method stub

	}

	@Override
	public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
		// TODO Auto-generated method stub

	}

	@Override
	public void seekToBeginning(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

	}

	@Override
	public void seekToEnd(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

	}

	@Override
	public long position(TopicPartition partition) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long position(TopicPartition partition, Duration timeout) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions, Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<TopicPartition> paused() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void pause(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

	}

	@Override
	public void resume(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch,
			Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConsumerGroupMetadata groupMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close(Duration timeout) {
		// TODO Auto-generated method stub

	}

	@Override
	public void wakeup() {
		// TODO Auto-generated method stub

	}

}
