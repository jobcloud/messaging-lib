<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

abstract class AbstractKafkaProducer implements ProducerInterface
{

    /**
     * @var RdKafkaProducer
     */
    protected $producer;

    /**
     * @var array
     */
    protected $producerTopics = [];

    /**
     * @var null|int
     */
    protected $pollTimeout;

    /**
     * AbstractKafkaProducer constructor.
     * @param RdKafkaProducer $producer
     * @param array           $brokerList
     * @param int|null        $pollTimeout
     */
    public function __construct(RdKafkaProducer $producer, array $brokerList, ?int $pollTimeout)
    {
        $this->producer = $producer;
        $this->producer->addBrokers(implode(',', $brokerList));
        $this->pollTimeout = $pollTimeout;
    }

    /**
     * @param string $topic
     * @return RdKafkaProducerTopic
     */
    public function getProducerTopicForTopic(string $topic): RdKafkaProducerTopic
    {
        if (!isset($this->producerTopics[$topic])) {
            $this->producerTopics[$topic] = $this->producer->newTopic($topic);
        }

        return $this->producerTopics[$topic];
    }
}
