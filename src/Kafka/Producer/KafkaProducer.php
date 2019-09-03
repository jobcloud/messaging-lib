<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaMessageException;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

final class KafkaProducer implements KafkaProducerInterface
{

    /** @var RdKafkaProducer */
    protected $producer;

    /** @var KafkaConfiguration */
    protected $kafkaConfiguration;

    /** @var array */
    protected $producerTopics = [];

    /** @var Registry|null */
    protected $schemaRegistry;

    /** @var RecordSerializer */
    protected $recordSerializer;

    /**
     * KafkaProducer constructor.
     * @param RdKafkaProducer    $producer
     * @param KafkaConfiguration $kafkaConfiguration
     * @param Registry           $schemaRegistry
     */
    public function __construct(
        RdKafkaProducer $producer,
        KafkaConfiguration $kafkaConfiguration,
        ?Registry $schemaRegistry = null
    ) {
        $this->producer = $producer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->schemaRegistry = $schemaRegistry;
    }

    /**
     * Produces a message to the topic and partition defined in the message
     * If a schema name was given, the message body will be avro serialized.
     *
     * @param MessageInterface         $message
     * @param KafkaAvroSchemaInterface $schema
     * @return void
     * @throws KafkaProducerException
     * @throws SchemaRegistryException
     */
    public function produce(MessageInterface $message, ?KafkaAvroSchemaInterface $schema = null): void
    {
        if (false === $message instanceof KafkaProducerMessageInterface) {
            throw new KafkaProducerException(
                sprintf(
                    KafkaProducerException::UNSUPPORTED_MESSAGE_EXCEPTION_MESSAGE,
                    KafkaProducerMessageInterface::class
                )
            );
        }

        try {
            $message = $this->getProducerMessage($message, $schema);
        } catch (SchemaRegistryException $e) {
            throw $e;
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
     * @param KafkaProducerMessageInterface $message
     * @param KafkaAvroSchemaInterface      $schema
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws KafkaMessageException
     */
    private function getProducerMessage(
        KafkaProducerMessageInterface $message,
        ?KafkaAvroSchemaInterface $schema = null
    ): KafkaProducerMessageInterface {
        if (null === $message->getBody()) {
            return $message;
        }

        if (null === $schemaRegistry = $this->getSchemaRegistry()) {
            return $message;
        }

        if (null === $schema) {
            return $message;
        }

        $body = json_decode($message->getBody(), true);

        if (null === $body) {
            throw new KafkaMessageException(KafkaMessageException::AVRO_BODY_MUST_BE_JSON_MESSAGE);
        }

        if (null === $schema->getVersion()) {
            $schemaDefinition = $schemaRegistry->latestVersion($schema->getSchemaName());
        } else {
            $schemaDefinition = $schemaRegistry->schemaForSubjectAndVersion(
                $schema->getSchemaName(),
                $schema->getVersion()
            );
        }

        $recordSerializer = $this->getRecordSerializer($schemaRegistry);

        try {
            $body = $recordSerializer->encodeRecord($schema->getSchemaName(), $schemaDefinition, $body);
        } catch (SchemaRegistryException $e) {
            throw $e;
        }

        return $message->withBody($body);
    }

    /**
     * @param Registry $schemaRegistry
     * @return RecordSerializer
     */
    private function getRecordSerializer(Registry $schemaRegistry): RecordSerializer
    {
        if (null === $this->recordSerializer) {
            $this->recordSerializer = new RecordSerializer($schemaRegistry);
        }

        return $this->recordSerializer;
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
     * Returns the schema registry if any was set
     *
     * @return Registry|null
     */
    public function getSchemaRegistry(): ?Registry
    {
        return $this->schemaRegistry;
    }
}
