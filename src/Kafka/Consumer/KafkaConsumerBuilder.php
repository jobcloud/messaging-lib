<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait;
use Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException;
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
     * Returns the builder
     *
     * @return KafkaConsumerBuilder
     */
    public static function create(): self
    {
        return new self();
    }

    /**
     * Add brokers from which you want to consume
     *
     * @param string $broker
     * @return KafkaConsumerBuilderInterface
     */
    public function addBroker(string $broker): KafkaConsumerBuilderInterface
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * Add topic name(s) (and additionally partitions and offsets) to subscribe to
     *
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
     * Add configuration settings, otherwise the kafka defaults apply
     *
     * @param array $config
     * @return KafkaConsumerBuilderInterface
     */
    public function addConfig(array $config): KafkaConsumerBuilderInterface
    {
        $this->config = $config + $this->config;

        return $this;
    }

    /**
     * Set the timeout for all consumer actions
     *
     * @param integer $timeout
     * @return KafkaConsumerBuilderInterface
     */
    public function setTimeout(int $timeout): KafkaConsumerBuilderInterface
    {
        $this->timeout = $timeout;

        return $this;
    }

    /**
     * Set the consumer group
     *
     * @param string $consumerGroup
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerGroup(string $consumerGroup): KafkaConsumerBuilderInterface
    {
        $this->consumerGroup = $consumerGroup;

        return $this;
    }

    /**
     * Set the consumer type, can be either CONSUMER_TYPE_LOW_LEVEL or CONSUMER_TYPE_HIGH_LEVEL
     *
     * @param string $consumerType
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumerType(string $consumerType): KafkaConsumerBuilderInterface
    {
        $this->consumerType = $consumerType;

        return $this;
    }

    /**
     * Set a callback to be called on errors.
     * The default callback will throw a KafkaBrokerException for every error
     *
     * @param callable $errorCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setErrorCallback(callable $errorCallback): KafkaConsumerBuilderInterface
    {
        $this->errorCallback = $errorCallback;

        return $this;
    }

    /**
     * Set a callback to be called on consumer rebalance
     *
     * @param callable $rebalanceCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setRebalanceCallback(callable $rebalanceCallback): KafkaConsumerBuilderInterface
    {
        $this->rebalanceCallback = $rebalanceCallback;

        return $this;
    }

    /**
     * Only applicable for the high level consumer
     * Callback that is going to be called when you call consume
     *
     * @param callable $consumeCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setConsumeCallback(callable $consumeCallback): KafkaConsumerBuilderInterface
    {
        $this->consumeCallback = $consumeCallback;

        return $this;
    }

    /**
     * Set callback that is being called on offset commits
     *
     * @param callable $offsetCommitCallback
     * @return KafkaConsumerBuilderInterface
     */
    public function setOffsetCommitCallback(callable $offsetCommitCallback): KafkaConsumerBuilderInterface
    {
        $this->offsetCommitCallback = $offsetCommitCallback;

        return $this;
    }

    /**
     * Returns your consumer instance
     *
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
