<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait;
use Jobcloud\Messaging\Kafka\Message\Normalizer\NormalizerInterface;
use Jobcloud\Messaging\Kafka\Message\Normalizer\NullNormalizer;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer as RdKafkaProducer;

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
     * @var int
     */
    private $pollTimeout = 1;

    /**
     * @var NormalizerInterface
     */
    private $normalizer;

    /**
     * KafkaProducerBuilder constructor.
     */
    private function __construct()
    {
        $this->deliverReportCallback = new KafkaProducerDeliveryReportCallback();
        $this->errorCallback = new KafkaErrorCallback();
        $this->normalizer = new NullNormalizer();
    }

    /**
     * Returns the producer builder
     *
     * @return KafkaProducerBuilderInterface
     */
    public static function create(): KafkaProducerBuilderInterface
    {
        return new self();
    }

    /**
     * Adds a broker to which you want to produce
     *
     * @param string $broker
     * @return KafkaProducerBuilderInterface
     */
    public function addBroker(string $broker): KafkaProducerBuilderInterface
    {
        $this->brokers[] = $broker;

        return $this;
    }

    /**
     * Add configuration settings, otherwise the kafka defaults apply
     *
     * @param array $config
     * @return KafkaProducerBuilderInterface
     */
    public function addConfig(array $config): KafkaProducerBuilderInterface
    {
        $this->config += $config;

        return $this;
    }

    /**
     * Sets callback for the delivery report. The broker will send a delivery
     * report for every message which describes if the delivery was successful or not
     *
     * @param callable $deliveryReportCallback
     * @return KafkaProducerBuilderInterface
     */
    public function setDeliveryReportCallback(callable $deliveryReportCallback): KafkaProducerBuilderInterface
    {
        $this->deliverReportCallback = $deliveryReportCallback;

        return $this;
    }

    /**
     * Set a callback to be called on errors.
     * The default callback will throw an exception for every error
     *
     * @param callable $errorCallback
     * @return KafkaProducerBuilderInterface
     */
    public function setErrorCallback(callable $errorCallback): KafkaProducerBuilderInterface
    {
        $this->errorCallback = $errorCallback;

        return $this;
    }

    /**
     * Sets the poll timeout. Poll queries and dispatches events to callbacks.
     *
     * @param integer $pollTimeout
     * @return KafkaProducerBuilderInterface
     */
    public function setPollTimeout(int $pollTimeout): KafkaProducerBuilderInterface
    {
        $this->pollTimeout = $pollTimeout;

        return $this;
    }

    /**
     * Lets you set a custom normalizer for produce message
     *
     * @param NormalizerInterface $normalizer
     * @return KafkaProducerBuilderInterface
     */
    public function setNormalizer(NormalizerInterface $normalizer): KafkaProducerBuilderInterface
    {
        $this->normalizer = $normalizer;

        return $this;
    }

    /**
     * Returns your producer instance
     *
     * @return ProducerInterface
     * @throws KafkaProducerException
     */
    public function build(): ProducerInterface
    {
        if ([] === $this->brokers) {
            throw new KafkaProducerException(KafkaProducerException::NO_BROKER_EXCEPTION_MESSAGE);
        }

        //Thread termination improvement (https://github.com/arnaud-lb/php-rdkafka#performance--low-latency-settings)
        $this->config['socket.timeout.ms'] = 50;
        $this->config['queue.buffering.max.ms'] = 1;

        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $this->config['internal.termination.signal'] = SIGIO;
            unset($this->config['queue.buffering.max.ms']);
        }

        $kafkaConfig = $this->createKafkaConfig($this->config, $this->brokers, [], $this->pollTimeout);

        //set producer callbacks
        $this->registerCallbacks($kafkaConfig);

        $rdKafkaProducer = new RdKafkaProducer($kafkaConfig);

        return new KafkaProducer($rdKafkaProducer, $kafkaConfig, $this->normalizer);
    }

    /**
     * @param KafkaConfiguration $conf
     * @return void
     */
    private function registerCallbacks(KafkaConfiguration $conf): void
    {
        $conf->setDrMsgCb($this->deliverReportCallback);
        $conf->setErrorCb($this->errorCallback);
    }
}
