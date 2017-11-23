<?php


namespace Jobcloud\Kafka\Producer;

use Jobcloud\Kafka\Helper\ConfigTrait;
use RdKafka\Producer;

abstract class AbstractProducer implements ProducerInterface
{
    use ConfigTrait;


    protected $config;

    protected $producer;

    protected $topic;



    public function __construct(array $brokerList, string $topic, array $config = [])
    {
        $this->config = $this->getConfig($config);
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

class bla extends Producer
{

}