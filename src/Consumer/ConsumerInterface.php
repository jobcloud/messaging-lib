<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

interface ConsumerInterface
{
    /**
     * @param integer $timeout
     * @return mixed
     */
    public function consume(int $timeout);
}
