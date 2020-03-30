# Migration from messaging-lib to php-kafka-lib

Internally not much has changed, we have mostly gotten rid of,  
the general interfaces, since we won't implement support for other  
messaging systems than Kafka.

In most cases you can just:
1. `composer remove jobcloud/messaging-lib`
2. `composer require jobcloud/php-kafka-lib ~0.1`
3. Replace namespace `Jobcloud\Messaging\Kafka` with `Jobcloud\Kafka`
4. In your project / code replace `ConsumerException` with `KafkaConsumerConsumeException`