#mvn -Dtest=fs.consumer.TestFSConsumerWithMockConsumer#testFSConsumer test -DjvmArgs="-XX:+UseG1GC"
mvn test -DjvmArgs="-XX:+UseG1GC"
