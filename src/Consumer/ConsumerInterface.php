<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

interface ConsumerInterface
{
    /**
     * @param integer $timeout Timeout to wait for new message in milliseconds
     * @return MessageInterface
     * @throws ConsumerException
     */
    public function consume(int $timeout): MessageInterface;
}
