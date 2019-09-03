<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Messaging\Kafka\Exception\KafkaMessageException;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
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

    /** @var array */
    protected $readerSchemas = [];

    /** @var RecordSerializer */
    protected $recordSerializer;

    /** @var Registry|null */
    protected $schemaRegistry;

    /**
     * @param mixed              $consumer
     * @param KafkaConfiguration $kafkaConfiguration
     * @param Registry           $schemaRegistry
     * @param array              $readerSchemas
     */
    public function __construct(
        $consumer,
        KafkaConfiguration $kafkaConfiguration,
        ?Registry $schemaRegistry,
        array $readerSchemas
    ) {
        $this->consumer = $consumer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->readerSchemas = $readerSchemas;
        $this->schemaRegistry = $schemaRegistry;
    }

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
     * @return Registry|null
     */
    public function getSchemaRegistry(): ?Registry
    {
        return $this->schemaRegistry;
    }

    /**
     * Consumes a message and returns it
     * In cases of errors / timeouts an exception is thrown
     *
     * @return MessageInterface
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerEndOfPartitionException
     * @throws KafkaConsumerTimeoutException
     * @throws SchemaRegistryException
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
            throw new KafkaConsumerEndOfPartitionException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        } elseif (RD_KAFKA_RESP_ERR__TIMED_OUT === $rdKafkaMessage->err) {
            throw new KafkaConsumerTimeoutException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        } elseif (null === $rdKafkaMessage->topic_name && RD_KAFKA_RESP_ERR_NO_ERROR !== $rdKafkaMessage->err) {
            throw new KafkaConsumerConsumeException($rdKafkaMessage->errstr(), $rdKafkaMessage->err);
        }

        try {
            $message = $this->getConsumerMessage($rdKafkaMessage);
        } catch (SchemaRegistryException $e) {
            throw $e;
        }

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
     * @param RdKafkaMessage $message
     * @return KafkaConsumerMessageInterface
     * @throws SchemaRegistryException
     * @throws KafkaMessageException
     */
    protected function getConsumerMessage(RdKafkaMessage $message): KafkaConsumerMessageInterface
    {
        if (null === $this->getSchemaRegistry() || false === isset($this->readerSchemas[$message->topic_name])) {
            return new KafkaConsumerMessage(
                $message->topic_name,
                $message->partition,
                $message->offset,
                $message->timestamp,
                $message->key,
                $message->payload,
                $message->headers
            );
        }

        $schemaRegistry = $this->getSchemaRegistry();

        /** @var KafkaAvroSchema $readerSchema */
        $readerSchema = $this->readerSchemas[$message->topic_name];

        if (null === $readerSchema->getVersion()) {
            $schema = $schemaRegistry->latestVersion($readerSchema->getSchemaName());
        } else {
            $schema = $schemaRegistry->schemaForSubjectAndVersion(
                $readerSchema->getSchemaName(),
                $readerSchema->getVersion()
            );
        }

        $recordSerializer = $this->getRecordSerializer($schemaRegistry);

        try {
            $body = json_encode($recordSerializer->decodeMessage($message->payload, $schema));
        } catch (SchemaRegistryException $e) {
            throw $e;
        }

        if (false === $body) {
            throw new KafkaMessageException(KafkaMessageException::UNABLE_TO_DECODE_PAYLOAD);
        }

        return new KafkaConsumerMessage(
            $message->topic_name,
            $message->partition,
            $message->offset,
            $message->timestamp,
            $message->key,
            $body,
            $message->headers
        );
    }

    /**
     * @param Registry $schemaRegistry
     * @return RecordSerializer
     */
    protected function getRecordSerializer(Registry $schemaRegistry): RecordSerializer
    {
        if (null === $this->recordSerializer) {
            $this->recordSerializer = new RecordSerializer($schemaRegistry);
        }

        return $this->recordSerializer;
    }

    /**
     * @param integer $timeout
     * @return null|RdKafkaMessage
     */
    abstract protected function kafkaConsume(int $timeout): ?RdKafkaMessage;
}
