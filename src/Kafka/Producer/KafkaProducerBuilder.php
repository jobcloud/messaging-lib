<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;

final class KafkaProducerBuilder implements KafkaProducerBuilderInterface
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
     * @var callable
     */
    private $deliverReportCallback;

    /**
     * @var callable
     */
    private $errorCallback;

    /**
     * KafkaProducerBuilder constructor.
     */
    private function __construct()
    {
        $this->setDeliveryReportCallback(new KafkaProducerDeliveryReportCallback());
        $this->setErrorCallback(new KafkaErrorCallback());
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
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @return array
     */
    public function getBrokers(): array
    {
        return $this->brokers;
    }

    /**
     * @return callable
     */
    public function getDeliveryReportCallback()
    {
        return $this->deliverReportCallback;
    }

    /**
     * @return callable
     */
    public function getErrorCallback()
    {
        return $this->errorCallback;
    }

    /**
     * @return ProducerInterface
     */
    public function build(): ProducerInterface
    {
        $kafkaConfig = $this->createKafkaConfig($this->getConfig());

        $kafkaConfig->setDrMsgCb($this->getDeliveryReportCallback());
        $kafkaConfig->setErrorCb($this->getErrorCallback());

        $rdKafkaProducer = new Producer($kafkaConfig);

        return new KafkaProducer($rdKafkaProducer, $this->brokers);
    }
}
