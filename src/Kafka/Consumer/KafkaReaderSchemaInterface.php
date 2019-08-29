<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface KafkaReaderSchemaInterface
{
    /**
     * @return string
     */
    public function getSchemaName(): string;

    /**
     * @return integer
     */
    public function getVersion(): ?int;
}
