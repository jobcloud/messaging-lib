<?php

namespace Jobcloud\Messaging\Producer;

interface ProducerInterface
{
    public function produce(string $message);
}
