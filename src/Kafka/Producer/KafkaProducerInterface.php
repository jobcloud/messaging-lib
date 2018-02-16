<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;

interface KafkaProducerInterface extends ProducerInterface
{
    /**
     * @param integer $timeout
     * @return void
     */
    public function poll(int $timeout);
}
