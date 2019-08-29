<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
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

    /** @var Registry */
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
        ?Registry $schemaRegistry
    ) {
        $this->producer = $producer;
        $this->kafkaConfiguration = $kafkaConfiguration;
        $this->schemaRegistry = $schemaRegistry;
    }

    /**
     * Produces a message to the topic and partition defined in the message
     * If a schema name was given, the message body will be avro serialized.
     *
     * @param MessageInterface $message
     * @param string|null      $schemaName
     * @param integer|null     $version
     * @return void
     * @throws KafkaProducerException
     * @throws SchemaRegistryException
     */
    public function produce(MessageInterface $message, ?string $schemaName = null, ?int $version = null): void
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

        try {
            $message = $this->getProducerMessage($message, $schemaName, $version);
        } catch (SchemaRegistryException $e) {
            throw $e;
        }

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
     * @param string|null                   $schemaName
     * @param integer|null                  $version
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     */
    private function getProducerMessage(
        KafkaProducerMessageInterface $message,
        ?string $schemaName = null,
        ?int $version = null
    ): KafkaProducerMessageInterface {
        if (null === $schemaRegistry = $this->getSchemaRegistry()) {
            return $message;
        }

        if (null === $schemaName) {
            return $message;
        }

        if (null === $version) {
            $schema = $schemaRegistry->latestVersion($schemaName);
        } else {
            $schema = $schemaRegistry->schemaForSubjectAndVersion($schemaName, $version);
        }

        $recordSerializer = $this->getRecordSerializer($schemaRegistry);

        return $message->withBody(
            $recordSerializer->encodeRecord(
                $schemaName,
                $schema,
                $message->getBody()
            )
        );
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
