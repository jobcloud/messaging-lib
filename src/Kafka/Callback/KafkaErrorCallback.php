<?php
// phpcs:ignoreFile
declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Callback;

use Jobcloud\Messaging\Kafka\Exception\KafkaBrokerException;

require_once __DIR__ . '/../Exception/KafkaBrokerException.php';  // @codeCoverageIgnore

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
        if (RD_KAFKA_RESP_ERR__TRANSPORT === $errorCode) {
            return;
        }

        throw new KafkaBrokerException(
            $reason,
            $errorCode
        );
    }
}
