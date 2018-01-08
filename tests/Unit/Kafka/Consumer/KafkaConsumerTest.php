<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use Jobcloud\Messaging\Kafka\Consumer\Message;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message as RdKafkaMessage;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer
 * @covers \Jobcloud\Messaging\Kafka\Consumer\AbstractKafkaConsumer
 */
class KafkaConsumerTest extends TestCase
{

    public function testConsumeSuccess()
    {
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->setMethods(['consume', 'unsubscribe', 'getSubscription'])
            ->disableOriginalConstructor()
            ->getMock();

        /** @var RdKafkaMessage|MockObject $messageMock */
        $messageMock = $this->getMockBuilder(RdKafkaMessage::class)
            ->setMethods(['errstr'])
            ->getMock();

        $messageMock->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $messageMock->topic_name = 'sample_topic';
        $messageMock->partition = 0;
        $messageMock->offset = 1;

        $messageMock
            ->expects(self::never())
            ->method('errstr');

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willReturn($messageMock);

        $consumerMock
            ->expects(self::any())
            ->method('unsubscribe')
            ->willReturn(null);

        $consumerMock
            ->expects(self::any())
            ->method('getSubscription')
            ->willReturn([]);

        $consumer = new KafkaConsumer($consumerMock, ['test'], 0);

        $message = $consumer->consume();

        self::assertInstanceOf(Message::class, $message);

        self::assertEquals($messageMock->payload, $message->getBody());
        self::assertEquals($messageMock->offset, $message->getOffset());
        self::assertEquals($messageMock->partition, $message->getPartition());
        self::assertEquals($messageMock->err, $message->getErrorCode());
        self::assertNull($message->getErrorMessage());
    }

    public function testConsumeFail()
    {
        $exceptionMessage = 'Unknown error';

        self::expectException(KafkaConsumerException::class);
        self::expectExceptionMessage($exceptionMessage);

        /** @var RdKafkaMessage|MockObject $messageMock */
        $messageMock = $this->getMockBuilder(RdKafkaMessage::class)
            ->setMethods(['errstr'])
            ->getMock();

        $messageMock->err = -1;

        $messageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn($exceptionMessage);

        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->setMethods(['consume', 'unsubscribe', 'getSubscription'])
            ->disableOriginalConstructor()
            ->getMock();

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willReturn($messageMock);

        $consumerMock
            ->expects(self::any())
            ->method('unsubscribe')
            ->willReturn(null);

        $consumerMock
            ->expects(self::any())
            ->method('getSubscription')
            ->willReturn([]);

        $consumer = new KafkaConsumer($consumerMock, ['test'], 0);

        $consumer->consume();
    }
}
