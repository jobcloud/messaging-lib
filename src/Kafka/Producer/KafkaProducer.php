<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Encoder\EncoderInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
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
     * @var KafkaConfiguration
     */
    protected $kafkaConfiguration;

    /**
     * @var array
     */
    protected $producerTopics = [];

    /**
     * @var EncoderInterface
     */
    protected $encoder;

    /**
     * KafkaProducer constructor.
     * @param RdKafkaProducer    $producer
     * @param KafkaConfiguration $kafkaConfiguration
     * @param EncoderInterface   $encoder
     */
    public function __construct(
        RdKafkaProducer $producer,
        KafkaConfiguration $kafkaConfiguration,
        EncoderInterface $encoder
    ) {
        $this->producer = $producer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->encoder = $encoder;
    }

    /**
     * Produces a message to the topic and partition defined in the message
     * If a schema name was given, the message body will be avro serialized.
     *
     * @param MessageInterface $message
     * @return void
     * @throws KafkaProducerException
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

        $message = $this->encoder->encode($message);

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
