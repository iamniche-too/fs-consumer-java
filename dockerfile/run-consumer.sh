export CONSUMER_GROUP_ID="consumerGroup0"

TOPIC_NAMES=""
for i in {1..50};
do 
  TOPIC_NAMES+="sensor$i" 
  if [ $i -ne 50 ]; then
    TOPIC_NAMES+="," 
  fi 
done

#echo "TOPIC_NAMES: ${TOPIC_NAMES}"
export TOPIC_NAMES
java -cp ./fs-consumer-java-0.0.1-jar-with-dependencies.jar fs.consumer.kafka.FSConsumerMain -XX:+UseG1GC
