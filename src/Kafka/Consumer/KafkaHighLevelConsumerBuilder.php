<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use RdKafka\KafkaConsumer;

final class KafkaHighLevelConsumerBuilder implements KafkaHighLevelConsumerBuilderInterface
{

    use KafkaConfigTrait;

    /**
     * @var string
     */
    private $broker;

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
    public function setBroker(string $broker): KafkaHighLevelConsumerBuilderInterface
    {
        $this->broker = $broker;

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
     * @throws KafkaConsumerBuilderException
     */
    public function build(): KafkaConsumer
    {
        if (null === $this->broker) {
            throw new KafkaConsumerBuilderException('No brokers to connect');
        }

        $this->config['group.id'] = $this->consumerGroup;
        $this->config['metadata.broker.list'] = $this->broker;

        $kafkaConfig = $this->createKafkaConfig($this->config);

        return new KafkaConsumer($kafkaConfig);
    }
}
