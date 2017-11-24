<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Kafka\Consumer\KafkaConsumerBuilderInterface;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use \RdKafka\KafkaConsumer as RdKafkaConsumer;

final class KafkaConsumerBuilder implements KafkaConsumerBuilderInterface
{

    use KafkaConfigTrait;

    /**
     * @param array $config
     * @return ConsumerInterface
     */
    public function createConsumer(array $config): ConsumerInterface
    {
        $kafkaConsumer = new RdKafkaConsumer($config);

        return new KafkaConsumer($kafkaConsumer);
    }

    /**
     * @return ConsumerInterface
     */
    public function build(): ConsumerInterface
    {
        $kafkaConfig = $this->createKafkaConfig($this->getConfig());

        $rdKafkaConsumer = new RdKafkaConsumer($kafkaConfig);

        return new KafkaConsumer($rdKafkaConsumer, $this->brokers, $this->topic, $this->config);
    }
}
