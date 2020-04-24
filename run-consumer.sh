export CONSUMER_GROUP_ID="consumerGroup0"
export BOOTSTRAP_SERVERS="localhost:9092"
export TOPIC_NAMES="sensor0"
java -cp ./target/fs-consumer-java-0.0.1-SNAPSHOT.jar fs.consumer.kafka.FSConsumerMain -XX:+UseG1GC
