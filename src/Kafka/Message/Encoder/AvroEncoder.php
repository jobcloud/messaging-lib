<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Encoder;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroEncoderException;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;

final class AvroEncoder implements EncoderInterface
{

    /**
     * @var AvroSchemaRegistryInterface
     */
    private $registry;

    /** @var RecordSerializer */
    private $recordSerializer;

    /**
     * @param AvroSchemaRegistryInterface $registry
     * @param RecordSerializer            $recordSerializer
     */
    public function __construct(AvroSchemaRegistryInterface $registry, RecordSerializer $recordSerializer)
    {
        $this->recordSerializer = $recordSerializer;
        $this->registry = $registry;
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws AvroEncoderException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        if (null === $producerMessage->getBody()) {
            return $producerMessage;
        }

        if (null === $avroSchema = $this->registry->getSchemaForTopic($producerMessage->getTopicName())) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                    $producerMessage->getTopicName()
                )
            );
        }

        if (null === $avroSchema->getDefinition()) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::UNABLE_TO_LOAD_DEFINITION_MESSAGE,
                    $avroSchema->getName()
                )
            );
        }

        $arrayBody = json_decode($producerMessage->getBody(), true, 512, JSON_THROW_ON_ERROR);

        $body = $this->recordSerializer->encodeRecord($avroSchema->getName(), $avroSchema->getDefinition(), $arrayBody);

        return $producerMessage->withBody($body);
    }
}
