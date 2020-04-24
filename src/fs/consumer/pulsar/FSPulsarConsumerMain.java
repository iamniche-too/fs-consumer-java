package fs.consumer.pulsar;

import java.util.Map;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

public class FSPulsarConsumerMain {

	public static String SERVICE_URL_CONFIG = "SERVICE_URL";
	
	public static String TOPIC_NAME_CONFIG = "TOPIC_NAME";
	
	public static String SUBSCRIPTION_NAME_CONFIG = "SUBSCRIPTION_NAME";
	
	public static String DEFAULT_SERVICE_URL = "pulsar://localhost:6650";
	
	public static void main(String[] args) throws PulsarClientException {
		Map<String, String> envMap = System.getenv();
		
		String serviceUrl;
		if (envMap.containsKey(SERVICE_URL_CONFIG)) {
			serviceUrl = envMap.get(SERVICE_URL_CONFIG);
		} else {
			// production = internal-service-0.kafka.svc.cluster.local:32400
			
			serviceUrl = SERVICE_URL_CONFIG;
			System.out.format("Warning -SERVICE_URL defaulting to =%s%n", serviceUrl);
		}
		
		String topicName;
		if (envMap.containsKey(TOPIC_NAME_CONFIG)) {
			topicName = envMap.get(TOPIC_NAME_CONFIG);
		} else {
			System.out.format("Warning - TOPIC_NAME missing from environment, defaulting to sensor0");
			topicName = "sensor0";
		}
		
		String subscriptionName;
		if (envMap.containsKey(TOPIC_NAME_CONFIG)) {
			subscriptionName = envMap.get(TOPIC_NAME_CONFIG);
		} else {
			System.out.format("Warning - SUBSCRIPTION_NAME missing from environment, defaulting to sensor0");
			subscriptionName = "sensor0";
		}
		
		PulsarClient client = PulsarClient.builder()
        .serviceUrl(serviceUrl)
        .build();
		
		FSPulsarConsumer fsConsumer = new FSPulsarConsumer(client, topicName, subscriptionName);
		Thread thread = new Thread(fsConsumer);
		thread.start();
	}
}
