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
     * @return KafkaConsumerBuilderInterface
     */
    public function addBroker(string $broker): KafkaConsumerBuilderInterface
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * @param TopicSubscriptionInterface $topicSubscription
     * @return KafkaConsumerBuilderInterface
     */
    public function addSubscription(TopicSubscriptionInterface $topicSubscription): KafkaConsumerBuilderInterface
    {
        $this->topics[] = $topicSubscription;

        return $this;
    }

    /**
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function setConfig(array $config): KafkaConsumerBuilderInterface
    {
        $this->config += $config;

        return $this;
    }

    /**
     * @param integer $timeout
     * @return KafkaConsumerBuilderInterface
     */
    public function setTimeout(int $timeout): KafkaConsumerBuilderInterface
    {
        $this->timeout = $timeout;

        return $this;
    }

    /**
     * @param string $consumerGroup
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): KafkaConsumerBuilderInterface
    {
        $this->consumerGroup = $consumerGroup;

        return $this;
    }

    /**
     * @param callable $errorCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setErrorCallback(callable $errorCallback): KafkaConsumerBuilderInterface
    {
        $this->errorCallback = $errorCallback;

        return $this;
    }

    /**
     * @param callable $rebalanceCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setRebalanceCallback(callable $rebalanceCallback): KafkaConsumerBuilderInterface
    {
        $this->rebalanceCallback = $rebalanceCallback;

        return $this;
    }

    /**
     * @return KafkaConsumerInterface
     * @throws KafkaConsumerBuilderException
     */
    public function build(): KafkaConsumerInterface
    {
        if ([] === $this->brokers) {
            throw new KafkaConsumerBuilderException('No brokers to connect');
        }

        if ([] === $this->topics) {
            throw new KafkaConsumerBuilderException('No topics set to consume');
        }

        //set additional config
        $this->config['group.id'] = $this->consumerGroup;
        $this->config['enable.auto.offset.store'] = false;

        //create config from given settings
        $kafkaConfig = $this->createKafkaConfig($this->config, $this->brokers, $this->topics, $this->timeout);

        //set consumer callbacks
        $kafkaConfig->setErrorCb($this->errorCallback);
        if (null !== $this->rebalanceCallback) {
            $kafkaConfig->setRebalanceCb($this->rebalanceCallback);
        }

        //create RdConsumer
        $rdKafkaConsumer = new RdKafkaConsumer($kafkaConfig);

        return new KafkaConsumer($rdKafkaConsumer, $kafkaConfig);
    }
}
