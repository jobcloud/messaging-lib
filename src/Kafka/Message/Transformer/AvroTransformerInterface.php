<?php


namespace Jobcloud\Messaging\Kafka\Message\Transformer;

use \AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;

interface AvroTransformerInterface
{
    /**
     * @param string     $schemaName
     * @param AvroSchema $schema
     * @param string     $value
     * @return string
     */
    public function encodeValue(string $schemaName, AvroSchema $schema, string $value): string;

    /**
     * @param string          $binaryValue
     * @param AvroSchema|null $schema
     * @return array
     */
    public function decodeValue(string $binaryValue, AvroSchema $schema = null): array;

    /**
     * @return Registry
     */
    public function getSchemaRegistry(): Registry;
}
