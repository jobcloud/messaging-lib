<?php

namespace Jobcloud\Messaging\Kafka\Message\Helper;

use \AvroSchema;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroDenormalizeException;
use Jobcloud\Messaging\Kafka\Exception\AvroNormalizerException;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;

/**
 * @property AvroTransformerInterface $avroTransformer
 */
trait SchemaRegistryHelperTrait
{

    /**
     * @param KafkaMessageInterface $kafkaMessage
     * @return AvroSchema|null
     *@throws SchemaRegistryException
     * @throws AvroDenormalizeException
     */
    private function getAvroSchemaDefinition(KafkaMessageInterface $kafkaMessage): ?AvroSchema
    {
        if (null === $avroSchema = $this->schemaMapping[$kafkaMessage->getTopicName()]) {
            return null;
        }

        if (false === $avroSchema instanceof KafkaAvroSchemaInterface) {
            throw new AvroNormalizerException(
                sprintf(
                    AvroNormalizerException::WRONG_SCHEMA_MAPPING_TYPE_MESSAGE,
                    $kafkaMessage->getTopicName(),
                    KafkaAvroSchemaInterface::class
                )
            );
        }

        /** @var KafkaAvroSchemaInterface $avroSchema */
        if (null === $avroSchema->getVersion()) {
            return $this->avroTransformer->getSchemaRegistry()->latestVersion($avroSchema->getSchemaName());
        }

        return $this->avroTransformer
            ->getSchemaRegistry()
            ->schemaForSubjectAndVersion($avroSchema->getSchemaName(), $avroSchema->getVersion());
    }
}
