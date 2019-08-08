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
     * @var array
     */
    private $topics = [];

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
     * @param array $config
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function setConfig(array $config): KafkaHighLevelConsumerBuilderInterface
    {
        $this->config += $config;

        return $this;
    }

    /**
     * @param TopicSubscriptionInterface $topicSubscription
     * @return KafkaHighLevelConsumerBuilderInterface
     */
    public function addSubscription(
        TopicSubscriptionInterface $topicSubscription
    ): KafkaHighLevelConsumerBuilderInterface {
        $this->topics[] = $topicSubscription;

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
        if ([] === $this->brokers) {
            throw new KafkaConsumerBuilderException('No brokers to connect');
        }

        if ([] === $this->topics) {
            throw new KafkaConsumerBuilderException('No topics set to consume');
        }

        $this->config['group.id'] = $this->consumerGroup;
        $this->config['metadata.broker.list'] = implode(',', $this->brokers);

        $kafkaConfig = $this->createKafkaConfig($this->config, $this->brokers, $this->topics, $this->timeout);

        return new KafkaConsumer($kafkaConfig);
    }
}
