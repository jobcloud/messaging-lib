<?php


namespace Jobcloud\Messaging\Kafka\Callback;

use RdKafka\KafkaConsumer as RdKafkaConsumer;
use Jobcloud\Messaging\Kafka\Exception\KafkaRebalanceException;

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
        switch ($errorCode) {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                $consumer->assign($partitions);
                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                $consumer->assign(null);
                break;

            default:
                $consumer->assign(null); // sync state
                throw new KafkaRebalanceException(KafkaRebalanceException::REBALANCE_EXCEPTION_MESSAGE, $errorCode);
                break;
        }
    }
}