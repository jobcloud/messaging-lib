<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use Jobcloud\Messaging\Consumer\ConsumerInterface;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaConsumerRebalanceCallback;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use \RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Exception as RdKafkaException;

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
        $this->rebalanceCallback = new KafkaConsumerRebalanceCallback();
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
     * @param string $topic
     * @return KafkaConsumerBuilder
     */
    public function subscribeToTopic(string $topic): self
    {
        $this->topics[] = $topic;

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
     * @return array
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @return string
     */
    public function getConsumerGroup() :string
    {
        return $this->consumerGroup;
    }

    /**
     * @return array
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @return ConsumerInterface
     * @throws KafkaConsumerException
     */
    public function build(): ConsumerInterface
    {
        $brokers = $this->getBrokers();

        if ([] === $brokers) {
            throw new KafkaConsumerException(KafkaConsumerException::NO_BROKER_EXCEPTION_MESSAGE);
        }

        //set additional config
        $this->config['group.id'] = $this->getConsumerGroup();
        $this->config['metadata.broker.list'] = implode(',', $brokers);

        //create config from given settings
        $kafkaConfig = $this->createKafkaConfig($this->getConfig());

        //set consumer callbacks
        $kafkaConfig->setErrorCb($this->errorCallback);
        $kafkaConfig->setRebalanceCb($this->rebalanceCallback);

        //create RdConsumer
        try {
            $rdKafkaConsumer = new RdKafkaConsumer($kafkaConfig);
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerException(
                sprintf(KafkaConsumerException::CREATION_EXCEPTION_MESSAGE, $e->getMessage()),
                $e->getCode(),
                $e
            );
        }

        return new KafkaConsumer($rdKafkaConsumer, $this->getTopics());
    }
}
