<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Decoder;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;
use Jobcloud\Messaging\Kafka\Exception\AvroDenormalizeException;

class AvroDecoder implements DecoderInterface
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
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     * @throws AvroDenormalizeException
     * @throws SchemaRegistryException
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        if (null === $consumerMessage->getBody()) {
            return $consumerMessage;
        }

        $avroSchema = $this->avroTransformer->getSchemaRegistry()->getSchemaForTopic($consumerMessage->getTopicName());

        $body = json_encode(
            $this->avroTransformer->decodeValue($consumerMessage->getBody(), $avroSchema->getDefinition()),
            JSON_THROW_ON_ERROR
        );

        return new KafkaConsumerMessage(
            $consumerMessage->getTopicName(),
            $consumerMessage->getPartition(),
            $consumerMessage->getOffset(),
            $consumerMessage->getTimestamp(),
            $consumerMessage->getKey(),
            $body,
            $consumerMessage->getHeaders()
        );
    }
}
