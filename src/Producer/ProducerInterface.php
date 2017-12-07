<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

interface ProducerInterface
{
    /**
     * @param string $message
     * @param string $topic
     * @return mixed|void
     */
    public function produce(string $message, string $topic);
}
