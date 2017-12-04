<?php

namespace Jobcloud\Messaging\Consumer;

interface ConsumerInterface
{
    /**
     * @param integer $timeout
     * @return mixed
     */
    public function consume(int $timeout);
}
