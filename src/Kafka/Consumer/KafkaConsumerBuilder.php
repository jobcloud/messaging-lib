<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerBuilderException;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;

final class KafkaConsumerBuilder implements KafkaConsumerBuilderInterface
{

    use KafkaConfigTrait;

    const CONSUMER_TYPE_LOW_LEVEL = 'low';
    const CONSUMER_TYPE_HIGH_LEVEL = 'high';

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
     * @param string  $topicName
     * @param array   $partitions
     * @param integer $offset
     * @return KafkaConsumerBuilderInterface
     */
    public function addSubscription(
        string $topicName,
        array $partitions = [],
        int $offset = self::OFFSET_STORED
    ): KafkaConsumerBuilderInterface {

        $this->topics[] = new TopicSubscription($topicName, $partitions, $offset);

        return $this;
    }

    /**
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function addConfig(array $config): KafkaConsumerBuilderInterface
    {
        $this->config = $config + $this->config;

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
    public function setConsumeCallback(callable $consumeCallback): KafkaConsumerBuilderInterface
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
            throw new KafkaConsumerBuilderException(KafkaConsumerBuilderException::NO_BROKER_EXCEPTION_MESSAGE);
        }

        if ([] === $this->topics) {
            throw new KafkaConsumerBuilderException(KafkaConsumerBuilderException::NO_TOPICS_EXCEPTION_MESSAGE);
        }

        //set additional config
        $this->config['group.id'] = $this->consumerGroup;
        $this->config['enable.auto.offset.store'] = false;

        //create config from given settings
        $kafkaConfig = $this->createKafkaConfig(
            $this->config,
            $this->brokers,
            $this->topics,
            $this->timeout
        );

        //set consumer callbacks
        $this->registerCallbacks($kafkaConfig);

        //create RdConsumer

        if (self::CONSUMER_TYPE_LOW_LEVEL == $this->consumerType) {
            $rdKafkaConsumer = new RdKafkaLowLevelConsumer($kafkaConfig);

            return new KafkaLowLevelConsumer($rdKafkaConsumer, $kafkaConfig);
        }

        $rdKafkaConsumer = new RdKafkaHighLevelConsumer($kafkaConfig);

        return new KafkaHighLevelConsumer($rdKafkaConsumer, $kafkaConfig);
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

        if (null !== $this->consumeCallback) {
            $conf->setConsumeCb($this->rebalanceCallback);
        }

        if (null !== $this->offsetCommitCallback) {
            $conf->setOffsetCommitCb($this->rebalanceCallback);
        }
    }
}
