<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Exception;

use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException
 */
class KafkaConsumerConsumeExceptionTest extends TestCase
{
    public function testGetAndConstructOfKafkaConsumerConsumeException()
    {
        $message = new KafkaConsumerMessage(
            'test-topic',
            1,
            42,
            1562324233704,
            'asdf-asdf-asfd-asdf',
            'some test content',
            [ 'key' => 'value' ]
        );

        $exception = new KafkaConsumerConsumeException('', 0, $message);

        self::assertSame($message, $exception->getKafkaMessage());
    }

    public function testGetAndConstructOfKafkaConsumerConsumeExceptionWithNullAsMessage()
    {
        $exception = new KafkaConsumerConsumeException('', 0, null);

        self::assertNull($exception->getKafkaMessage());
    }
}
