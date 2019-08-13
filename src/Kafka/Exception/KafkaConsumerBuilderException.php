<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaConsumerBuilderException extends \Exception
{
    const NO_BROKER_EXCEPTION_MESSAGE = 'You need add at least one broker to connect to.';
    const NO_TOPICS_EXCEPTION_MESSAGE = 'No topics defined to subscribe to.';
    const UNSUPPORTED_CALLBACK_EXCEPTION_MESSAGE = 'The callback %s is not supported for %s';
}
