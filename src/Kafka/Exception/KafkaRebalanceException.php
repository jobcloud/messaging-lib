<?php

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaRebalanceException extends \Exception
{
    const REBALANCE_EXCEPTION_MESSAGE = 'Error during rebalance of consumer';
}
