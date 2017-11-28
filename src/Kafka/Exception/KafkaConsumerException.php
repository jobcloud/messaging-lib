<?php

namespace Jobcloud\Messaging\Kafka\Exception;

class KafkaConsumerException extends \Exception
{
    const CONSUMPTION_EXCEPTION_MESSAGE = 'Error during message consumption: %s';
    const CREATION_EXCEPTION_MESSAGE = 'Error during instantiation of consumer: %s';
}
