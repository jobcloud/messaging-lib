<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;

final class KafkaProducerBuilder implements KafkaProducerBuilderInterface
{

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

    public static function create()
    {
        return new self();
    }

    public function setTopic(string $topic): self
    {
        $this->topic = $topic;

        return $this;
    }

    public function addBroker(string $broker): self
    {
        $this->brokers[] = $broker;

        return $this;
    }

    public function setConfig(array $config): self
    {
        $this->config = $config;

        return $this;
    }

    public function build(): ProducerInterface
    {
        $rdKafkaProducer = new Producer();

        return new KafkaProducer($rdKafkaProducer, $this->brokers, $this->topic, $this->config);
    }
}
