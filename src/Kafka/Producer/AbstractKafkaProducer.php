<?php


namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

abstract class AbstractKafkaProducer implements ProducerInterface
{

    protected $producer;

    protected $producerTopics = [];

    /**
     * AbstractKafkaProducer constructor.
     * @param Producer $producer
     * @param array    $brokerList
     */
    public function __construct(Producer $producer, array $brokerList)
    {
        $this->producer = $producer;
        $this->producer->addBrokers(implode(',', $brokerList));
    }

    /**
     * @param string $topic
     * @return ProducerTopic
     */
    public function getProducerTopicForTopic(string $topic): ProducerTopic
    {
        if (!isset($this->producerTopics[$topic])) {
            $this->producerTopics[$topic] = $this->producer->newTopic($topic);
        }

        return $this->producersTopic[$topic];
    }

    /**
     * @param string $message
     * @param string $topic
     * @param string|NULL $key
     */
    public function produce(string $message, string $topic, string $key = null)
    {
        $topicProducer = $this->getProducerTopicForTopic($topic);

        $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $message, $key);
    }
}
