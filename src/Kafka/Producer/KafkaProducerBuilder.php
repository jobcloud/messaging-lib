<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;

final class KafkaProducerBuilder implements KafkaProducerBuilderInterface
{
    use KafkaConfigTrait;

    /**
     * @var string
     */
    private $topic;

    /**
     * @var array
     */
    private $brokers = [];

    /**
     * @var array
     */
    private $config = [];

    private function __construct()
    {
    }

    /**
     * @return KafkaProducerBuilder
     */
    public static function create()
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
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @return ProducerInterface
     */
    public function build(): ProducerInterface
    {
        $kafkaConfig = $this->createKafkaConfig($this->getConfig());

        $rdKafkaProducer = new Producer($kafkaConfig);

        return new KafkaProducer($rdKafkaProducer, $this->brokers);
    }
}
