<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

interface ConsumerInterface
{
    /**
     * Consumes a message and returns it
     * @return MessageInterface The consumed message
     * @throws ConsumerException
     */
    public function consume(): MessageInterface;
}
