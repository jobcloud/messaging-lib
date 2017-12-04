<?php

namespace Jobcloud\Messaging\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException;

final class KafkaErrorCallback
{

    /**
     * @param mixed   $kafka
     * @param integer $errorCode
     * @param string  $reason
     * @return void
     * @throws KafkaBrokerException
     */
    public function __invoke($kafka, int $errorCode, string $reason)
    {
        throw new KafkaBrokerException(
            $reason,
            $errorCode
        );
    }
}
