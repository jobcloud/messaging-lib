<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Encoder;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroNormalizerException;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;

class AvroEncoder implements EncoderInterface
{

    /** @var AvroTransformerInterface */
    private $avroTransformer;

    /** @var array */
    private $schemaMapping;

    /**
     * @param AvroTransformerInterface $avroTransformer
     * @param array                    $schemaMapping
     */
    public function __construct(AvroTransformerInterface $avroTransformer, array $schemaMapping)
    {
        $this->avroTransformer = $avroTransformer;
        $this->schemaMapping = $schemaMapping;
    }

    /**
     * @param KafkaProducerMessageInterface $producerMessage
     * @return KafkaProducerMessageInterface
     * @throws SchemaRegistryException
     * @throws AvroNormalizerException
     */
    public function encode(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        if (null === $producerMessage->getBody()) {
            return $producerMessage;
        }

        $registry = $this->avroTransformer->getSchemaRegistry();

        if (null === $avroSchema = $registry->getSchemaForTopic($producerMessage->getTopicName())) {
            throw new AvroNormalizerException(
                sprintf(
                    AvroNormalizerException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                    $producerMessage->getTopicName()
                )
            );
        }

        $arrayBody = json_decode($producerMessage->getBody(), true);

        if (null === $arrayBody) {
            throw new AvroNormalizerException(AvroNormalizerException::MESSAGE_BODY_MUST_BE_JSON_MESSAGE);
        }

        $body = $this->avroTransformer->encodeValue($avroSchema->getName(), $avroSchema->getDefinition(), $arrayBody);

        return $producerMessage->withBody($body);
    }
}
