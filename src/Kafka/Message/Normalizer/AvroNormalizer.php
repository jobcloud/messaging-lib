<?php

namespace Jobcloud\Messaging\Kafka\Message\Normalizer;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroNormalizerException;
use Jobcloud\Messaging\Kafka\Message\Helper\SchemaRegistryHelperTrait;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;

class AvroNormalizer implements NormalizerInterface
{

    use SchemaRegistryHelperTrait;

    /** @var AvroTransformerInterface */
    private $avroTransformer;

    /** @var array|KafkaAvroSchemaInterface[] */
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
    public function normalize(KafkaProducerMessageInterface $producerMessage): KafkaProducerMessageInterface
    {
        if (null === $producerMessage->getBody()) {
            return $producerMessage;
        }

        if (null === $avroSchema = $this->schemaMapping[$producerMessage->getTopicName()]) {
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

        $schemaDefinition = $this->getAvroSchemaDefinition($producerMessage);

        $body = $this->avroTransformer->encodeValue($avroSchema->getSchemaName(), $schemaDefinition, $arrayBody);

        return $producerMessage->withBody($body);
    }
}
