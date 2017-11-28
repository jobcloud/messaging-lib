<?php


namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Exception as RdKafkaException;

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

        return $this->producersTopics[$topic];
    }

    /**
     * @param string $message
     * @param string $topic
     * @throws KafkaProducerException
     * @return void
     */
    public function produce(string $message, string $topic)
    {
        $topicProducer = $this->getProducerTopicForTopic($topic);

        try {
            $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $message);
        } catch (RdKafkaException $e) {
            throw new KafkaProducerException(
                sprintf(KafkaProducerException::PRODUCTION_EXCEPTION_MESSAGE, $e->getMessage()),
                $e->getCode(),
                $e
            );
        }

    }
}
