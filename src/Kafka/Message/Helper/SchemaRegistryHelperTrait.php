<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Helper;

use \AvroSchema;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use Jobcloud\Messaging\Kafka\Exception\AvroEncoderException;
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
     */
    private function getAvroSchemaDefinition(KafkaMessageInterface $kafkaMessage): ?AvroSchema
    {
        if (false === isset($this->schemaMapping[$kafkaMessage->getTopicName()])) {
            return null;
        }

        $avroSchema = $this->schemaMapping[$kafkaMessage->getTopicName()];

        if (false === $avroSchema instanceof KafkaAvroSchemaInterface) {
            throw new AvroEncoderException(
                sprintf(
                    AvroEncoderException::WRONG_SCHEMA_MAPPING_TYPE_MESSAGE,
                    $kafkaMessage->getTopicName(),
                    KafkaAvroSchemaInterface::class
                )
            );
        }

        /** @var KafkaAvroSchemaInterface $avroSchema */
        if (null === $avroSchema->getVersion()) {
            return $this->avroTransformer->getSchemaRegistry()->latestVersion($avroSchema->getName());
        }

        return $this->avroTransformer
            ->getSchemaRegistry()
            ->schemaForSubjectAndVersion($avroSchema->getName(), $avroSchema->getVersion());
    }
}
