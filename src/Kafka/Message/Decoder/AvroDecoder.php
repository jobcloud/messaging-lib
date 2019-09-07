<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Decoder;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;
use Jobcloud\Messaging\Kafka\Exception\AvroDenormalizeException;

final class AvroDecoder implements DecoderInterface
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
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     * @throws AvroDenormalizeException
     * @throws SchemaRegistryException
     */
    public function decode(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        $schemaDefinition = null;

        if (null === $consumerMessage->getBody()) {
            return $consumerMessage;
        }

        $avroSchema = $this->avroTransformer->getSchemaRegistry()->getSchemaForTopic($consumerMessage->getTopicName());

        if (true === $avroSchema instanceof KafkaAvroSchemaInterface) {
            $schemaDefinition = $avroSchema->getDefinition();
        }

        $body = json_encode(
            $this->avroTransformer->decodeValue($consumerMessage->getBody(), $schemaDefinition),
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
