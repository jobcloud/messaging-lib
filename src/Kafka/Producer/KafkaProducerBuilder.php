<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;

final class KafkaProducerBuilder implements KafkaProducerBuilderInterface
{
    use KafkaConfigTrait;

    /**
     * @var array|string[]
     */
    private $brokers = [];

    /**
     * @var array
     */
    private $config = [];

    /**
     * @var callable
     */
    private $deliverReportCallback;

    /**
     * @var callable
     */
    private $errorCallback;

    /**
     * @var null|int
     */
    private $pollTimeout = null;

    /**
     * KafkaProducerBuilder constructor.
     */
    private function __construct()
    {
        $this->deliverReportCallback = new KafkaProducerDeliveryReportCallback();
        $this->errorCallback = new KafkaErrorCallback();
    }

    /**
     * @return KafkaProducerBuilder
     */
    public static function create(): self
    {
        return new self();
    }

    /**
     * @param string $broker
     * @return KafkaProducerBuilder
     */
    public function addBroker(string $broker): self
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * @param array $config
     * @return KafkaProducerBuilder
     */
    public function setConfig(array $config): self
    {
        $this->config += $config;

        return $this;
    }

    /**
     * @param callable $deliveryReportCallback
     * @return KafkaProducerBuilder
     */
    public function setDeliveryReportCallback(callable $deliveryReportCallback): self
    {
        $this->deliverReportCallback = $deliveryReportCallback;

        return $this;
    }

    /**
     * @param callable $errorCallback
     * @return KafkaProducerBuilder
     */
    public function setErrorCallback(callable $errorCallback): self
    {
        $this->errorCallback = $errorCallback;

        return $this;
    }

    /**
     * @param int|null $pollTimeout
     */
    public function setPollTimeout(?int $pollTimeout): void
    {
        $this->pollTimeout = $pollTimeout;
    }

    /**
     * @return ProducerInterface
     * @throws KafkaProducerException
     */
    public function build(): ProducerInterface
    {
        if ([] === $this->brokers) {
            throw new KafkaProducerException(KafkaProducerException::NO_BROKER_EXCEPTION_MESSAGE);
        }

        $kafkaConfig = $this->createKafkaConfig($this->config);

        $kafkaConfig->setDrMsgCb($this->deliverReportCallback);
        $kafkaConfig->setErrorCb($this->errorCallback);

        $rdKafkaProducer = new Producer($kafkaConfig);

        return new KafkaProducer($rdKafkaProducer, $this->brokers, $this->pollTimeout);
    }
}
