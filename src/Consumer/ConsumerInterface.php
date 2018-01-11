<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

interface ConsumerInterface
{
    /**
     * Consumes a message and returns it
     * @return MessageInterface|null The consumed message or null of there isn't any
     * @throws ConsumerException
     */
    public function consume(): ?MessageInterface;
}
