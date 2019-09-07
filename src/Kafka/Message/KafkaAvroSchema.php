<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

final class KafkaAvroSchema implements KafkaAvroSchemaInterface
{

    /**
     * @var string
     */
    private $name;

    /**
     * @var integer|null
     */
    private $version;

    /**
     * KafkaAvroSchema constructor.
     * @param string       $schemaName
     * @param integer|null $version
     */
    public function __construct(string $schemaName, ?int $version = null)
    {
        $this->name = $schemaName;
        $this->version = $version;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * @return integer
     */
    public function getVersion(): ?int
    {
        return $this->version;
    }
}
