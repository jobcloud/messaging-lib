<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

final class KafkaProducer implements ProducerInterface
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
     * @var int
     */
    protected $pollTimeout;

    /**
     * @var array
     */
    protected $brokers;

    /**
     * @var boolean
     */
    protected $isConnected = false;

    /**
     * AbstractKafkaProducer constructor.
     * @param RdKafkaProducer $producer
     * @param array           $brokers
     * @param integer         $pollTimeout
     */
    public function __construct(RdKafkaProducer $producer, array $brokers, int $pollTimeout)
    {
        $this->producer = $producer;
        $this->brokers = $brokers;
        $this->pollTimeout = $pollTimeout;
    }

    /**
     * @param string  $message
     * @param string  $topic
     * @param integer $partition
     * @return void
     */
    public function produce(string $message, string $topic, int $partition = RD_KAFKA_PARTITION_UA)
    {
        $this->connectProducerToBrokers();

        $topicProducer = $this->getProducerTopicForTopic($topic);

        $topicProducer->produce($partition, 0, $message);

        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll($this->pollTimeout);
        }
    }

    /**
     * @param string $topic
     * @return RdKafkaProducerTopic
     */
    private function getProducerTopicForTopic(string $topic): RdKafkaProducerTopic
    {
        if (!isset($this->producerTopics[$topic])) {
            $this->producerTopics[$topic] = $this->producer->newTopic($topic);
        }

        return $this->producerTopics[$topic];
    }

    /**
     * @return void
     */
    private function connectProducerToBrokers(): void
    {
        if (true === $this->isConnected) {
            return;
        }

        $this->producer->addBrokers(implode(',', $this->brokers));
        $this->isConnected = true;
    }
}
