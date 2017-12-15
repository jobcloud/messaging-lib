<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

use RdKafka\Message as RdKafkaMessage;

interface ConsumerInterface
{
    /**
     * @param integer $timeout
     * @return RdKafkaMessage
     * @throws ConsumerException
     */
    public function consume(int $timeout);
}
