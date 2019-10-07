<?php

declare(strict_types=1);

// phpcs:disable PSR12.Properties.ConstantVisibility.NotFound

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaRebalanceException extends \Exception
{
    const REBALANCE_EXCEPTION_MESSAGE = 'Error during rebalance of consumer';
}
