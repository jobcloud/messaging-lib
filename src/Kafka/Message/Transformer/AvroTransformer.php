<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Transformer;

use FlixTech\SchemaRegistryApi\Registry;
use \Throwable;
use \AvroSchema;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;

/**
 * @codeCoverageIgnore
 */
final class AvroTransformer extends RecordSerializer implements AvroTransformerInterface
{

    /** @var Registry */
    protected $registry;

    /**
     * @param Registry $registry
     * @param array    $options
     */
    public function __construct(Registry $registry, array $options = [])
    {
        parent::__construct($registry, $options);
        $this->registry = $registry;
    }

    /**
     * @param string     $schemaName
     * @param AvroSchema $schema
     * @param array      $value
     * @return string
     * @throws SchemaRegistryException
     */
    public function encodeValue(string $schemaName, AvroSchema $schema, array $value): string
    {
        return $this->encodeRecord($schemaName, $schema, $value);
    }

    /**
     * @param string          $binaryValue
     * @param AvroSchema|null $schema
     * @throws Throwable
     * @return array
     */
    public function decodeValue(string $binaryValue, AvroSchema $schema = null): array
    {
        return $this->decodeMessage($binaryValue, $schema);
    }

    /**
     * @return Registry
     */
    public function getSchemaRegistry(): Registry
    {
        return $this->registry;
    }
}
