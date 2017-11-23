<?php


namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;

abstract class AbstractKafkaProducer implements ProducerInterface
{

    protected $config;

    protected $producer;

    protected $topic;

    public function __construct(Producer $producer, array $brokerList, string $topic, array $config)
    {
        $this->producer = new Producer($this->config);
        $this->producer->addBrokers(implode(',', $brokerList));
        $this->topic = $topic;

        $x = $this->producer->newTopic("x");
    }

    public function getProducerForTopic (string $topic)
    {

    }

    public function produce(string $message)
    {
        // TODO: Implement produce() method.
    }
}
