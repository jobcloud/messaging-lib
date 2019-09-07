<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message\Transformer;

use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistryInterface;
use \Throwable;
use \AvroSchema;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;

/**
 * @codeCoverageIgnore
 */
class AvroTransformer extends RecordSerializer implements AvroTransformerInterface
{

    /** @var AvroSchemaRegistryInterface */
    protected $registry;

    /**
     * @param AvroSchemaRegistryInterface $registry
     * @param array                       $options
     */
    public function __construct(AvroSchemaRegistryInterface $registry, array $options = [])
    {
        parent::__construct($registry->getRegistry(), $options);
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
     * @return AvroSchemaRegistryInterface
     */
    public function getSchemaRegistry(): AvroSchemaRegistryInterface
    {
        return $this->registry;
    }
}
