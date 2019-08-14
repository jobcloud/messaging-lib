<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

final class KafkaProducer implements ProducerInterface
{

    /** @var RdKafkaProducer */
    protected $producer;

    /** @var KafkaConfiguration */
    protected $kafkaConfiguration;

    /** @var array */
    protected $producerTopics = [];

    /**
     * KafkaProducer constructor.
     * @param RdKafkaProducer    $producer
     * @param KafkaConfiguration $kafkaConfiguration
     */
    public function __construct(RdKafkaProducer $producer, KafkaConfiguration $kafkaConfiguration)
    {
        $this->producer = $producer;
        $this->kafkaConfiguration = $kafkaConfiguration;
    }

    /**
     * Produces a message to the topic and partition defined in the message
     *
     * @param MessageInterface $message
     * @throws KafkaProducerException
     * @return void
     */
    public function produce(MessageInterface $message): void
    {
        if (false === $message instanceof KafkaProducerMessageInterface) {
            throw new KafkaProducerException(
                sprintf(
                    KafkaProducerException::UNSUPPORTED_MESSAGE_EXCEPTION_MESSAGE,
                    KafkaProducerMessageInterface::class
                )
            );
        }

        /** @var KafkaProducerMessageInterface $message */
        $topicProducer = $this->getProducerTopicForTopic($message->getTopicName());

        $topicProducer->producev(
            $message->getPartition(),
            0,
            $message->getBody(),
            $message->getKey(),
            $message->getHeaders()
        );

        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll($this->kafkaConfiguration->getTimeout());
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
}
