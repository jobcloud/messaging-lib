<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\TopicPartition;

final class KafkaConsumerBuilder implements KafkaConsumerBuilderInterface
{

    use KafkaConfigTrait;

    const CONSUMER_TYPE_LOW_LEVEL = KafkaLowLevelConsumer::class;
    const CONSUMER_TYPE_HIGH_LEVEL = KafkaHighLevelConsumer::class;
    private const RD_KAFKA_CONSUMER_TYPE_LOW_LEVEL = RdKafkaLowLevelConsumer::class;
    private const RD_KAFKA_CONSUMER_TYPE_HIGH_LEVEL = RdKafkaHighLevelConsumer::class;

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
     * @var string
     */
    private $consumerType = self::CONSUMER_TYPE_HIGH_LEVEL;

    /**
     * @var string
     */
    private $rdKafkaConsumerType = self::RD_KAFKA_CONSUMER_TYPE_HIGH_LEVEL;

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
     * @var callable
     */
    private $consumeCallback;

    /**
     * @var callable
     */
    private $offsetCommitCallback;

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
     * @return void
     */
    private function convertSubscriptionsToLowLevel(): void
    {
        $topicSubscriptions = [];

        foreach ($this->topics as $topic) {
            $topicSubscriptions[] = new TopicSubscription($topic);
        }

        $this->topics = $topicSubscriptions;
    }

    /**
     * @param string $topicName
     * @return KafkaConsumerBuilderInterface
     */
    public function addSubscription(string $topicName): KafkaConsumerBuilderInterface
    {
        $this->topics[] = $topicName;

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
     * @param string $consumerType
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerType(string $consumerType): KafkaConsumerBuilderInterface
    {
        $this->consumerType = $consumerType;

        if (self::CONSUMER_TYPE_HIGH_LEVEL === $consumerType) {
            $this->rdKafkaConsumerType = self::RD_KAFKA_CONSUMER_TYPE_HIGH_LEVEL;
        } elseif (self::CONSUMER_TYPE_LOW_LEVEL === $consumerType) {
            $this->rdKafkaConsumerType = self::RD_KAFKA_CONSUMER_TYPE_LOW_LEVEL;
        }

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
     * @param callable $consumeCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumeCb(callable $consumeCallback): KafkaConsumerBuilderInterface
    {
        $this->consumeCallback = $consumeCallback;

        return $this;
    }

    /**
     * @param callable $offsetCommitCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setOffsetCommitCallback(callable $offsetCommitCallback): KafkaConsumerBuilderInterface
    {
        $this->offsetCommitCallback = $offsetCommitCallback;

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

        if (self::CONSUMER_TYPE_LOW_LEVEL === $this->consumerType) {
            $this->convertSubscriptionsToLowLevel();
        }

        //create config from given settings
        $kafkaConfig = $this->createKafkaConfig($this->config, $this->brokers, $this->topics, $this->timeout);

        //set consumer callbacks
        $this->registerCallbacks($kafkaConfig);

        //create RdConsumer
        $rdKafkaConsumer = new $this->rdKafkaConsumerType($kafkaConfig);

        return new $this->consumerType($rdKafkaConsumer, $kafkaConfig);
    }

    /**
     * @param KafkaConfiguration $conf
     * @return void
     */
    private function registerCallbacks(KafkaConfiguration $conf): void
    {
        $conf->setErrorCb($this->errorCallback);

        if (null !== $this->rebalanceCallback) {
            $conf->setRebalanceCb($this->rebalanceCallback);
        }

        if (null !== $this->rebalanceCallback) {
            $conf->setConsumeCb($this->rebalanceCallback);
        }

        if (null !== $this->rebalanceCallback) {
            $conf->setOffsetCommitCb($this->rebalanceCallback);
        }
    }
}
