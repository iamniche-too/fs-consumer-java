# run specific test
# mvn -Dtest=fs.consumer.TestFSConsumerWithMockConsumer#testFSConsumer test -DjvmArgs="-XX:+UseG1GC"
# mvn -Dtest=fs.consumer.TestFSConsumerWithKafkaConsumer#testFSConsumer test -DjvmArgs="-XX:+UseG1GC"

# run all tests with default GC (CMS)
# mvn test

# run all tests with G1GC 
mvn test -DjvmArgs="-XX:+UseG1GC"
