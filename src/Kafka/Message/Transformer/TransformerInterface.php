<?php


namespace Jobcloud\Messaging\Kafka\Message\Transformer;

use \AvroSchema;

interface TransformerInterface
{
    public function encodeValue(string $subject, AvroSchema $schema, $record): string;

    public function decodeValue(string $binaryMessage, AvroSchema $readersSchema = null)
}
