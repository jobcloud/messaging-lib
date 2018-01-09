<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumeException;
use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use Jobcloud\Messaging\Kafka\Consumer\Message;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Exception as RdKafkaException;
use RdKafka\KafkaConsumer as RdKafkaConsumer;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\TopicPartition;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer
 */
class KafkaConsumerTest extends TestCase
{

    public function testConsumeSuccess()
    {
        $consumerMock = $this->getRdKafkaConsumerMock();

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

        $consumer = new KafkaConsumer($consumerMock, ['test'], 0);

        $message = $consumer->consume();

        self::assertInstanceOf(Message::class, $message);

        self::assertEquals($messageMock->payload, $message->getBody());
        self::assertEquals($messageMock->offset, $message->getOffset());
        self::assertEquals($messageMock->partition, $message->getPartition());
    }

    public function testConsumeFailThrowsException()
    {
        $exceptionMessage = 'Unknown error';

        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage($exceptionMessage);

        /** @var RdKafkaMessage|MockObject $messageMock */
        $messageMock = $this->getMockBuilder(RdKafkaMessage::class)
            ->setMethods(['errstr'])
            ->getMock();

        $messageMock->err = -1;
        $messageMock->partition = 1;
        $messageMock->offset = 42;
        $messageMock->topic_name = 'test';

        $messageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn($exceptionMessage);

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willReturn($messageMock);

        $consumer = new KafkaConsumer($consumerMock, ['test'], 0);

        $consumer->consume();
    }

    public function testConsumeConvertsExtensionExceptionToLibraryException()
    {
        $exceptionMessage = 'Something went wrong';

        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage($exceptionMessage);

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willThrowException(new RdKafkaException($exceptionMessage));

        $consumer = new KafkaConsumer($consumerMock, ['test'], 0);

        $consumer->consume();
    }

    public function testGetTopicsReturnsSubscribableTopicsOfConsumerInstance()
    {
        $topics = ['test'];

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumer = new KafkaConsumer($consumerMock, $topics, 0);

        self::assertSame($topics, $consumer->getTopics());
    }

    public function testSubscribeCallsRdKafkaConsumerSubscribeMethod()
    {
        $topics = ['test'];

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with($topics)
            ->willReturn(null);

        $consumer = new KafkaConsumer($consumerMock, $topics, 0);

        $consumer->subscribe();

        return $consumerMock;
    }

    public function testSubscribeConvertsExtensionExceptionToLibraryException()
    {
        $exceptionMessage = 'foobar';

        self::expectException(KafkaConsumerSubscriptionException::class);
        self::expectExceptionMessage($exceptionMessage);

        $topics = ['test'];

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::once())
            ->method('subscribe')
            ->with($topics)
            ->willThrowException(new RdKafkaException($exceptionMessage));

        $consumer = new KafkaConsumer($consumerMock, $topics, 0);

        $consumer->subscribe();

        return $consumerMock;
    }

    public function testUnsubscribeConvertsExtensionExceptionToLibraryException()
    {
        $exceptionMessage = 'foobar';

        self::expectException(KafkaConsumerSubscriptionException::class);
        self::expectExceptionMessage($exceptionMessage);

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::exactly(2))
            ->method('unsubscribe')
            ->willThrowException(new RdKafkaException($exceptionMessage));

        $consumer = new KafkaConsumer($consumerMock, [], 0);

        $consumer->unsubscribe();
    }

    public function testCommitWithoutMessagesDelegatesGeneralCommit()
    {
        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::once())
            ->method('commit')
            ->with()
            ->willReturn(null);

        $consumer = new KafkaConsumer($consumerMock, [], 0);

        $consumer->commit();
    }

    public function testCommitWithMessageOnlyCommitsGivenMessage()
    {
        $partition = 1;
        $offset = 42;
        $topic = 'topic';

        $message = new Message('some message', $topic, $partition, $offset);

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::once())
            ->method('commit')
            ->willReturnCallback(function ($topicPartitions) use ($partition, $offset, $topic, $message) {
                self::assertCount(1, $topicPartitions);

                $topicPartition = $topicPartitions[0];

                self::assertInstanceOf(TopicPartition::class, $topicPartition);

                self::assertEquals($partition, $message->getPartition());
                self::assertEquals($offset, $message->getOffset());
                self::assertEquals($topic, $message->getTopicName());
            });

        $consumer = new KafkaConsumer($consumerMock, [], 0);

        $consumer->commit($message);
    }

    public function testCommitWithInvalidObjectThrowsExceptionAndDoesNotTriggerCommit()
    {
        self::expectException(KafkaConsumerCommitException::class);
        self::expectExceptionMessage(
            'Provided message (offset: 0) is not an instance of "Jobcloud\Messaging\Kafka\Consumer\Message"'
        );

        $message = new \stdClass();

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::never())
            ->method('commit');

        $consumer = new KafkaConsumer($consumerMock, [], 0);

        $consumer->commit($message);
    }

    public function testDonNotTreatNonStoredOffsetAsAnError()
    {
        $partition = 1;
        $offset = 42;
        $topic = 'topic';

        $message = new Message('some message', $topic, $partition, $offset);

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::once())
            ->method('commit')
            ->willThrowException(new RdKafkaException('foo', RD_KAFKA_RESP_ERR__NO_OFFSET));

        $consumer = new KafkaConsumer($consumerMock, [], 0);

        $consumer->commit($message);
    }

    public function testCommitConvertsExtensionExceptionToLibraryException()
    {
        $exceptionCode = RD_KAFKA_RESP_ERR__FS;
        $exceptionMessage = 'foo';
        
        self::expectException(KafkaConsumerCommitException::class);
        self::expectExceptionMessage($exceptionMessage);
        self::expectExceptionCode($exceptionCode);

        $partition = 1;
        $offset = 42;
        $topic = 'topic';

        $message = new Message('some message', $topic, $partition, $offset);

        $consumerMock = $this->getRdKafkaConsumerMock();

        $consumerMock
            ->expects(self::once())
            ->method('commit')
            ->willThrowException(new RdKafkaException($exceptionMessage, $exceptionCode));

        $consumer = new KafkaConsumer($consumerMock, [], 0);

        $consumer->commit($message);
    }

    /**
     * @return RdKafkaConsumer|MockObject
     */
    private function getRdKafkaConsumerMock(): RdKafkaConsumer
    {
        /** @var RdKafkaConsumer|MockObject $consumerMock */
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->setMethods(['consume', 'subscribe', 'unsubscribe', 'getSubscription', 'commit'])
            ->disableOriginalConstructor()
            ->getMock();

        $consumerMock
            ->expects(self::any())
            ->method('unsubscribe')
            ->willReturn(null);

        $consumerMock
            ->expects(self::any())
            ->method('getSubscription')
            ->willReturn([]);

        return $consumerMock;
    }
}
