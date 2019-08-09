<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Exception;

use Jobcloud\Messaging\Kafka\Message\KafkaMessage;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException
 */
class KafkaConsumerConsumeExceptionTest extends TestCase
{
    public function testGetAndConstructOfKafkaConsumerConsumeException()
    {
        $key = '1234-1234-1234';
        $body = 'foo bar baz';
        $topic = 'test';
        $offset = 42;
        $partition = 1;
        $timestamp = 1562324233704;
        $headers = [ 'key' => 'value' ];

        $message = new KafkaMessage($key, $body, $topic, $partition, $offset, $timestamp, $headers);

        $exception = new KafkaConsumerConsumeException('', 0, $message);

        self::assertSame($message, $exception->getKafkaMessage());
    }

    public function testGetAndConstructOfKafkaConsumerConsumeExceptionWithNullAsMessage()
    {
        $exception = new KafkaConsumerConsumeException('', 0, null);

        self::assertNull($exception->getKafkaMessage());
    }
}
