<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Exception;

use Jobcloud\Messaging\Kafka\Consumer\Message;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException
 */
class KafkaConsumerConsumeExceptionTest extends TestCase
{
    public function testGetAndConstructOfKafkaConsumerConsumeException()
    {
        $message = $this->getMockBuilder(Message::class)
            ->disableOriginalConstructor()
            ->getMock();

        $exception = new KafkaConsumerConsumeException('', 0, $message);

        self::assertSame($message, $exception->getKafkaMessage());
    }

    public function testGetAndConstructOfKafkaConsumerConsumeExceptionWithNullAsMessage()
    {
        $exception = new KafkaConsumerConsumeException('', 0, NULL);

        self::assertNull($exception->getKafkaMessage());
    }
}
