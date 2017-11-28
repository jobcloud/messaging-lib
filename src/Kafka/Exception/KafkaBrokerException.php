<?php

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaBrokerException extends \Exception
{
    const BROKER_EXCEPTION_MESSAGE = 'The broker threw the following exception: %s';
}
