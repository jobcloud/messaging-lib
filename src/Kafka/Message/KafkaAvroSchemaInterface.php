<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

interface KafkaAvroSchemaInterface
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
