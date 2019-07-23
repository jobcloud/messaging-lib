<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use RdKafka\KafkaConsumer;

final class KafkaHighLevelConsumerBuilder implements KafkaHighLevelConsumerBuilderInterface
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
     * @var int
     */
    private $timeout = 1000;

    /**
     * @var string
     */
    private $consumerGroup = 'default';

    /**
     * @return KafkaHighLevelConsumerBuilder
     */
    public static function create(): self
    {
        return new self();
    }

    /**
     * @param string $broker
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function addBroker(string $broker): KafkaHighLevelConsumerBuilderInterface
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * @param integer $timeout
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setTimeout(int $timeout): KafkaHighLevelConsumerBuilderInterface
    {
        $this->timeout = $timeout;

        return $this;
    }

    /**
     * @param array $config
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConfig(array $config): KafkaHighLevelConsumerBuilderInterface
    {
        $this->config += $config;

        return $this;
    }

    /**
     * @param string $consumerGroup
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): KafkaHighLevelConsumerBuilderInterface
    {
        $this->consumerGroup = $consumerGroup;

        return $this;
    }

    /**
     * @return KafkaConsumer
     */
    public function build(): KafkaConsumer
    {
        $this->config['group.id'] = $this->consumerGroup;

        $kafkaConfig = $this->createKafkaConfig($this->config);

        return new KafkaConsumer($kafkaConfig);
    }
}
