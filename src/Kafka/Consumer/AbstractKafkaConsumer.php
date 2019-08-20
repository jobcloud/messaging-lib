<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Message as RdKafkaMessage;

abstract class AbstractKafkaConsumer implements KafkaConsumerInterface
{

    /** @var KafkaConfiguration */
    protected $kafkaConfiguration;

    /** @var boolean */
    protected $subscribed = false;

    /** @var RdKafkaLowLevelConsumer|RdKafkaHighLevelConsumer */
    protected $consumer;

    /**
     * Returns true if the consumer has subscribed to its topics, otherwise false
     * It is mandatory to call `subscribe` before `consume`
     *
     * @return boolean
     */
    public function isSubscribed(): bool
    {
        return $this->subscribed;
    }

    /**
     * Returns the configuration settings for this consumer instance as array
     *
     * @return array
     */
    public function getConfiguration(): array
    {
        return $this->kafkaConfiguration->dump();
    }

    /**
     * Consumes a message and returns it
     * In cases of errors / timeouts an exception is thrown
     *
     * @return MessageInterface
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerTimeoutException
     */
    public function consume(): MessageInterface
    {
        if (false === $this->isSubscribed()) {
            throw new KafkaConsumerConsumeException(KafkaConsumerConsumeException::NOT_SUBSCRIBED_EXCEPTION_MESSAGE);
        }

        if (null === $rdKafkaMessage = $this->kafkaConsume($this->kafkaConfiguration->getTimeout())) {
            throw new KafkaConsumerEndOfPartitionException(
                rd_kafka_err2str(RD_KAFKA_RESP_ERR__PARTITION_EOF),
                RD_KAFKA_RESP_ERR__PARTITION_EOF
            );
        }

        if (RD_KAFKA_RESP_ERR__PARTITION_EOF === $rdKafkaMessage->err) {
            throw new KafkaConsumerEndOfPartitionException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        } elseif (RD_KAFKA_RESP_ERR__TIMED_OUT === $rdKafkaMessage->err) {
            throw new KafkaConsumerTimeoutException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        } elseif (null === $rdKafkaMessage->topic_name && RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        }

        $message = new KafkaConsumerMessage(
            $rdKafkaMessage->topic_name,
            $rdKafkaMessage->partition,
            $rdKafkaMessage->offset,
            $rdKafkaMessage->timestamp,
            $rdKafkaMessage->key,
            $rdKafkaMessage->payload,
            $rdKafkaMessage->headers
        );

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err, $message);
        }

        return $message;
    }

    /**
     * Queries the broker for metadata on a certain topic
     *
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
