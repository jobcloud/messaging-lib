<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

use Jobcloud\Messaging\Consumer\ConsumerException;

class KafkaConsumerException extends ConsumerException
{
    const CONSUMPTION_EXCEPTION_MESSAGE = 'Error during message consumption: %s';
    const CREATION_EXCEPTION_MESSAGE = 'Error during instantiation of consumer: %s';
    const NO_BROKER_EXCEPTION_MESSAGE = 'You must define at least one broker';
}
