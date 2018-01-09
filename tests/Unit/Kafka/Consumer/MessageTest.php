<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\Message;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\Message
 */
class MessageTest extends TestCase
{
    public function testMessageGettersAndConstructor()
    {
        $body = 'foo bar baz';
        $topic = 'test';
        $offset = 42;
        $partition = 1;

        $message = new Message($body, $topic, $partition, $offset);

        self::assertEquals($body, $message->getBody());
        self::assertEquals($topic, $message->getTopicName());
        self::assertEquals($offset, $message->getOffset());
        self::assertEquals($partition, $message->getPartition());
    }
}
