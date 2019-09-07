<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Encoder;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroEncoderException;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;

final class AvroEncoder implements EncoderInterface
{

    /** @var AvroTransformerInterface */
    private $avroTransformer;

    /**
     * @param AvroTransformerInterface $avroTransformer
     */
    public function __construct(AvroTransformerInterface $avroTransformer)
    {
        $this->avroTransformer = $avroTransformer;
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

        $registry = $this->avroTransformer->getSchemaRegistry();

        if (null === $avroSchema = $registry->getSchemaForTopic($producerMessage->getTopicName())) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                    $producerMessage->getTopicName()
                )
            );
        }

        $arrayBody = json_decode($producerMessage->getBody(), true);

        if (null === $arrayBody) {
            throw new AvroEncoderException(AvroEncoderException::MESSAGE_BODY_MUST_BE_JSON_MESSAGE);
        }

        $body = $this->avroTransformer->encodeValue($avroSchema->getName(), $avroSchema->getDefinition(), $arrayBody);

        return $producerMessage->withBody($body);
    }
}
