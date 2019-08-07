<?php


namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;

interface KafkaProducerInterface extends ProducerInterface
{

    /**
     * @param integer $purgeFlags
     * @return integer
     */
    public function purge(int $purgeFlags): int;
}
