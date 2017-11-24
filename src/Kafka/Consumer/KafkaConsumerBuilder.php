<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use \RdKafka\KafkaConsumer as RdKafkaConsumer;

final class KafkaConsumerBuilder implements KafkaConsumerBuilderInterface
{

    use KafkaConfigTrait;

    /**
     * @var array
     */
    private $brokers = [];

    /**
     * @var array
     */
    private $config = [];

    /**
     * @var array
     */
    private $topics = [];

    /**
     * @var string
     */
    private $consumerGroup = "default";

    /**
     * KafkaConsumerBuilder constructor.
     */
    private function __construct()
    {
    }

    /**
     * @return KafkaConsumerBuilder
     */
    public static function create()
    {
        return new self();
    }

    /**
     * @param string $broker
     * @return KafkaConsumerBuilder
     */
    public function addBroker(string $broker): self
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * @param string $topic
     * @return KafkaConsumerBuilder
     */
    public function subscribeToTopic(string $topic): self
    {
        $this->topics[] = $topic;

        return $this;
    }

    /**
     * @param string $consumerGroup
     * @return KafkaConsumerBuilder
     */
    public function setConsumerGroup(string $consumerGroup): self
    {
        $this->consumerGroup = $consumerGroup;

        return $this;
    }

    /**
     * @param array $config
     * @return KafkaConsumerBuilder
     */
    public function setConfig(array $config): self
    {
        $this->config += $config;

        return $this;
    }

    /**
     * @return ConsumerInterface
     */
    public function build(): ConsumerInterface
    {
        $this->config['groupId'] = $this->consumerGroup;
        $this->config['metadata.broker.list'] = implode(',', $this->brokers);
        $kafkaConfig = $this->createKafkaConfig($this->getConfig());

        $rdKafkaConsumer = new RdKafkaConsumer($kafkaConfig);

        return new KafkaConsumer($rdKafkaConsumer, $this->brokers, $this->topics, $this->consumerGroup);
    }
}
