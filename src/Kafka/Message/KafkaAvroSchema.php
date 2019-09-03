<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

class KafkaAvroSchema implements KafkaAvroSchemaInterface
{

    /** @var string */
    private $schemaName;

    /** @var integer|null */
    private $version;

    /**
     * KafkaAvroSchema constructor.
     * @param string       $schemaName
     * @param integer|null $version
     */
    public function __construct(string $schemaName, ?int $version = null)
    {
        $this->schemaName = $schemaName;
        $this->version = $version;
    }

    /**
     * @return string
     */
    public function getSchemaName(): string
    {
        return $this->schemaName;
    }

    /**
     * @return integer
     */
    public function getVersion(): ?int
    {
        return $this->version;
    }
}
