<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Message\KafkaMessage;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\KafkaMessage
 */
final class KafkaMessageTest extends TestCase
{
    public function testMessageGettersAndConstructor()
    {
        $key = '1234-1234-1234';
        $body = 'foo bar baz';
        $topic = 'test';
        $offset = 42;
        $partition = 1;
        $timestamp = 1562324233704;
        $headers = [ 'key' => 'value' ];

        $message = new KafkaMessage($key, $body, $topic, $partition, $offset, $timestamp, $headers);

        self::assertEquals($key, $message->getKey());
        self::assertEquals($body, $message->getBody());
        self::assertEquals($topic, $message->getTopicName());
        self::assertEquals($offset, $message->getOffset());
        self::assertEquals($partition, $message->getPartition());
        self::assertEquals($timestamp, $message->getTimestamp());
        self::assertEquals($headers, $message->getHeaders());
    }
}
