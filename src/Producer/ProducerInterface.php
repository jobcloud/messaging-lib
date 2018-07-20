<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

interface ProducerInterface
{

    /**
     * @param string      $message
     * @param string      $topic
     * @param integer     $partition
     * @param string|null $key
     * @return void
     */
    public function produce(string $message, string $topic, int $partition, string $key = null);
}
