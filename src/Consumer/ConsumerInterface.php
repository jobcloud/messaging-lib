<?php

namespace Jobcloud\Messaging\Consumer;

interface ConsumerInterface
{
    public function consume(int $timeout);

    public function subscribe(array $topics);
}