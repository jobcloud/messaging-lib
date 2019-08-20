<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Callback;

use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Exception\KafkaRebalanceException;
use RdKafka\Exception as RdKafkaException;

require_once __DIR__ . '/../Exception/KafkaRebalanceException.php';

final class KafkaConsumerRebalanceCallback
{

    /**
     * @param RdKafkaConsumer $consumer
     * @param integer         $errorCode
     * @param array|null      $partitions
     * @throws KafkaRebalanceException
     * @return void
     */
    public function __invoke(RdKafkaConsumer $consumer, int $errorCode, array $partitions = null)
    {
        try {
            switch ($errorCode) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $consumer->assign($partitions);
                    break;

                default:
                    $consumer->assign(null);
                    break;
            }
        } catch (RdKafkaException $e) {
            throw new KafkaRebalanceException($e->getMessage(), $e->getCode(), $e);
        }
    }
}
