<?php

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaProducerException extends \Exception
{
    const PRODUCTION_EXCEPTION_MESSAGE = 'Error during message production: %s';
    const TIMEOUT_EXCEPTION_MESSAGE = 'Error message timed out: %s';
    const UNEXPECTED_EXCEPTION_MESSAGE = 'Unexpected error during production: %s';
}
