<?php

namespace Jobcloud\Messaging\Kafka\Exception;

use \RuntimeException;

class AvroNormalizerException extends RuntimeException
{
    const MESSAGE_BODY_MUST_BE_JSON_MESSAGE = 'The body of an avro message must be JSON';
    const NO_SCHEMA_FOR_TOPIC_MESSAGE = 'There is no avro schema defined for the topic %s';
    const WRONG_SCHEMA_MAPPING_TYPE_MESSAGE = 'The schema mapping for topic %s must be of instance %s';
}
