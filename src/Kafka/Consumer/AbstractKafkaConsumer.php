<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Message\KafkaMessage;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Message as RdKafkaMessage;

abstract class AbstractKafkaConsumer implements KafkaConsumerInterface
{

    /** @var int */
    const OFFSET_BEGINNING = RD_KAFKA_OFFSET_BEGINNING;
    /** @var int */
    const OFFSET_END = RD_KAFKA_OFFSET_END;
    /** @var int */
    const OFFSET_STORED = RD_KAFKA_OFFSET_STORED;

    /** @var KafkaConfiguration */
    protected $kafkaConfiguration;

    /** @var boolean */
    protected $subscribed = false;

    /** @var RdKafkaLowLevelConsumer|RdKafkaHighLevelConsumer */
    protected $consumer;

    /**
     * @return boolean
     */
    public function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    /**
     * @return KafkaConfiguration
     */
    public function getConfiguration(): KafkaConfiguration
    {
        return $this->kafkaConfiguration;
    }

    /**
     * @return MessageInterface
     * @throws KafkaConsumerConsumeException
     */
    public function consume(): MessageInterface
    {
        if (false === $this->isSubscribed()) {
            throw new KafkaConsumerConsumeException(KafkaConsumerConsumeException::NOT_SUBSCRIBED_EXCEPTION_MESSAGE);
        }

        if (null === $rdKafkaMessage = $this->kafkaConsume($this->kafkaConfiguration->getTimeout())) {
            throw new KafkaConsumerConsumeException(
                rd_kafka_err2str(RD_KAFKA_RESP_ERR__TIMED_OUT),
                RD_KAFKA_RESP_ERR__TIMED_OUT
            );
        }

        if ($rdKafkaMessage->topic_name === null && RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        }

        $message = new KafkaMessage(
            $rdKafkaMessage->key,
            $rdKafkaMessage->payload,
            $rdKafkaMessage->topic_name,
            $rdKafkaMessage->partition,
            $rdKafkaMessage->offset,
            $rdKafkaMessage->timestamp,
            $rdKafkaMessage->headers
        );

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        }

        return $message;
    }

    /**
     * @param RdKafkaConsumerTopic $topic
     * @return RdKafkaMetadataTopic
     * @throws RdKafkaException
     */
    public function getMetadataForTopic(RdKafkaConsumerTopic $topic): RdKafkaMetadataTopic
    {
        return $this->consumer
            ->getMetadata(
                false,
                $topic,
                $this->kafkaConfiguration->getTimeout()
            )
            ->getTopics()
            ->current();
    }

    /**
     * @param integer $timeout
     * @return null|RdKafkaMessage
     */
    abstract protected function kafkaConsume(int $timeout): ?RdKafkaMessage;
}
