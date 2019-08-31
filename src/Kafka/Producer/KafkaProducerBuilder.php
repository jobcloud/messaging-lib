<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use Jobcloud\Messaging\Kafka\Callback\KafkaErrorCallback;
use Jobcloud\Messaging\Kafka\Callback\KafkaProducerDeliveryReportCallback;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfigTrait;
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
     * @var Registry|null
     */
    private $schemaRegistry;

    /**
     * KafkaProducerBuilder constructor.
     */
    private function __construct()
    {
        $this->deliverReportCallback = new KafkaProducerDeliveryReportCallback();
        $this->errorCallback = new KafkaErrorCallback();
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
     * @param string $registryUrl
     * @return KafkaProducerBuilderInterface
     */
    public function addSchemaRegistryUrl(string $registryUrl): KafkaProducerBuilderInterface
    {
        $this->schemaRegistry = new CachedRegistry(
            new BlockingRegistry(
                new PromisingRegistry(
                    new Client(['base_uri' => $registryUrl])
                )
            ),
            new AvroObjectCacheAdapter()
        );

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
     * Returns your producer instance
     *
     * @return KafkaProducerInterface
     * @throws KafkaProducerException
     */
    public function build(): KafkaProducerInterface
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

        return new KafkaProducer($rdKafkaProducer, $kafkaConfig, $this->schemaRegistry);
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
