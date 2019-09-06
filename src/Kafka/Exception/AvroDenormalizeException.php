<?php

namespace Jobcloud\Messaging\Kafka\Exception;

use \RuntimeException;

class AvroDenormalizeException extends RuntimeException
{
    const UNABLE_TO_ENCODE_PAYLOAD = 'Was unable to JSON encode the decoded avro message';
}
