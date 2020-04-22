export POD_NAME="local"
export BOOTSTRAP_SERVERS="localhost:9092"
java -jar ./target/fs-consumer-java-0.0.1-SNAPSHOT.jar -XX:+UseG1GC 
