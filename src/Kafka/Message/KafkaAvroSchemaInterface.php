<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Message;

interface KafkaAvroSchemaInterface
{
    /**
     * @return string
     */
    public function getName(): string;

    /**
     * @return integer|null
     */
    public function getVersion(): ?int;
}
