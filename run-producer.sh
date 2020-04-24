export BOOTSTRAP_SERVERS="localhost:9092"
export TOPIC_NAMES="sensor0"
export MESSAGE_SIZE="750"
export RATE_LIMIT="75000"
java -cp ./target/fs-consumer-java-0.0.1-SNAPSHOT.jar fs.producer.kafka.FSProducerMain -XX:+UseG1GC
