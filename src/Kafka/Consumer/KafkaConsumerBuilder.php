<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use RdKafka\Consumer as RdKafkaConsumer;

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
    private $consumerGroup = 'default';

    /**
     * @var int
     */
    private $timeout = 1000;

    /**
     * @var callable
     */
    private $errorCallback;

    /**
     * @var callable
     */
    private $rebalanceCallback;

    /**
     * KafkaConsumerBuilder constructor.
     */
    private function __construct()
    {
        $this->errorCallback = new KafkaErrorCallback();
    }

    /**
     * @return KafkaConsumerBuilder
     */
    public static function create(): self
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
     * @param TopicSubscriptionInterface $topicSubscription
     * @return KafkaConsumerBuilder
     */
    public function addSubscription(TopicSubscriptionInterface $topicSubscription): self
    {
        $this->topics[] = $topicSubscription;

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
     * @param integer $timeout
     * @return KafkaConsumerBuilder
     */
    public function setTimeout(int $timeout): self
    {
        $this->timeout = $timeout;

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
     * @param callable $errorCallback
     * @return KafkaConsumerBuilder
     */
    public function setErrorCallback(callable $errorCallback): self
    {
        $this->errorCallback = $errorCallback;

        return $this;
    }

    /**
     * @param callable $rebalanceCallback
     * @return KafkaConsumerBuilder
     */
    public function setRebalanceCallback(callable $rebalanceCallback): self
    {
        $this->rebalanceCallback = $rebalanceCallback;

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

        //set additional config
        $this->config['group.id'] = $this->consumerGroup;

        //create config from given settings
        $kafkaConfig = $this->createKafkaConfig($this->config);

        //set consumer callbacks
        $kafkaConfig->setErrorCb($this->errorCallback);

        if (null !== $this->rebalanceCallback) {
            $kafkaConfig->setRebalanceCb($this->rebalanceCallback);
        }

        //create RdConsumer
        $rdKafkaConsumer = new RdKafkaConsumer($kafkaConfig);
        $rdKafkaConsumer->addBrokers(implode(',', $this->brokers));

        return new KafkaConsumer($rdKafkaConsumer, $this->topics, $this->timeout);
    }
}
