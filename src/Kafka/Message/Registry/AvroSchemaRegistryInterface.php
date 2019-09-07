<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Registry;

use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;

interface AvroSchemaRegistryInterface
{

    /**
     * @param string                   $topicName
     * @param KafkaAvroSchemaInterface $avroSchema
     * @return void
     */
    public function addSchemaMappingForTopic(string $topicName, KafkaAvroSchemaInterface $avroSchema): void;

    /**
     * @return Registry
     */
    public function getRegistry(): Registry;

    /**
     * @param string $topicName
     * @return KafkaAvroSchemaInterface|null
     * @throws SchemaRegistryException
     */
    public function getSchemaForTopic(string $topicName): ?KafkaAvroSchemaInterface;
}
