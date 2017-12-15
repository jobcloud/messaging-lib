<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Producer;

interface ProducerInterface
{

    /**
     * @param string $message
     * @param string $topic
     * @param int    $partition
     * @return mixed|void
     */
    public function produce(string $message, string $topic, int $partition = RD_KAFKA_PARTITION_UA);
}
