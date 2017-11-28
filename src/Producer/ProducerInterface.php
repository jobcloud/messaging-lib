<?php

namespace Jobcloud\Messaging\Producer;

interface ProducerInterface
{
    /**
     * @param string $message
     * @param string $topic
     * @return mixed
     */
    public function produce(string $message, string $topic);
}
