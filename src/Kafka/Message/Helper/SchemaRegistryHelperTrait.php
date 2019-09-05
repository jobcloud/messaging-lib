<?php

namespace Jobcloud\Messaging\Kafka\Message\Helper;

use \AvroSchema;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroDenormalizeException;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;

/**
 * @property AvroTransformerInterface $avroTransformer
 */
trait SchemaRegistryHelperTrait
{

    /**
     * @param KafkaConsumerMessageInterface $kafkaMessage
     * @return AvroSchema|null
     *@throws SchemaRegistryException
     * @throws AvroDenormalizeException
     */
    private function getAvroSchemaDefinition(KafkaConsumerMessageInterface $kafkaMessage): ?AvroSchema
    {
        if (null === $avroSchema = $this->schemaMapping[$kafkaMessage->getTopicName()]) {
            return null;
        }

        if (false === $avroSchema instanceof KafkaAvroSchemaInterface) {
            throw new AvroDenormalizeException(
                sprintf(
                    AvroDenormalizeException::UNKNOWN_SCHEMA_DEFINITION_MESSAGE,
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
