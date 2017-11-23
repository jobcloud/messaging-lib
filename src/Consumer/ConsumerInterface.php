<?php

namespace Jobcloud\Kafka\Consumer;

interface ConsumerInterface
{
    public function consume(int $timeout);

    public function subscribe(array $topics);
}