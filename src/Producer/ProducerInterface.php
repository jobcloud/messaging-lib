<?php

namespace Jobcloud\Kafka\Producer;

interface ProducerInterface
{
    public function produce(string $message);
}