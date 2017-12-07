<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;
use RdKafka\ProducerTopic;

abstract class AbstractKafkaProducer implements ProducerInterface
{

    /**
     * @var Producer
     */
    protected $producer;

    /**
     * @var array
     */
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

        return $this->producerTopics[$topic];
    }

    /**
     * @return integer
     */
    public function getPartition()
    {
        return RD_KAFKA_PARTITION_UA;
    }
}
