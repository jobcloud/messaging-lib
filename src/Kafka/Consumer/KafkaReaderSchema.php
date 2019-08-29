<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

class KafkaReaderSchema implements KafkaReaderSchemaInterface
{

    /** @var string */
    private $schemaName;

    /** @var integer|null */
    private $version;

    /**
     * KafkaReaderSchema constructor.
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
