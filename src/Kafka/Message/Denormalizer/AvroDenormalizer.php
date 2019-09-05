<?php

namespace Jobcloud\Messaging\Kafka\Message\Denormalizer;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Message\Helper\SchemaRegistryHelperTrait;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;
use Jobcloud\Messaging\Kafka\Exception\AvroDenormalizeException;

class AvroDenormalizer implements DenormalizerInterface
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
     * @param KafkaConsumerMessageInterface $consumerMessage
     * @return KafkaConsumerMessageInterface
     * @throws AvroDenormalizeException
     * @throws SchemaRegistryException
     */
    public function denormalize(KafkaConsumerMessageInterface $consumerMessage): KafkaConsumerMessageInterface
    {
        $schemaDefinition = $this->getAvroSchemaDefinition($consumerMessage);

        $body = json_encode($this->avroTransformer->decodeValue($consumerMessage->getBody(), $schemaDefinition));

        if (false === $body) {
            throw new AvroDenormalizeException(AvroDenormalizeException::UNABLE_TO_ENCODE_PAYLOAD);
        }

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
