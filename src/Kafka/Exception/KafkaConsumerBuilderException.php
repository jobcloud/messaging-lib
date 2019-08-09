<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaConsumerBuilderException extends \Exception
{
    const NO_BROKER_EXCEPTION_MESSAGE = 'No brokers defined to connect to.';
    const NO_TOPICS_EXCEPTION_MESSAGE = 'No topics defined to subscribe to.';
}
