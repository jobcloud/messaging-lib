<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer;
use Jobcloud\Messaging\Kafka\Consumer\Message;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\ConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Consumer as RdKafkaConsumer;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\Metadata;
use RdKafka\Queue as RdKafkaQueue;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumer
 */
final class KafkaConsumerTest extends TestCase
{

    public function testConsumeWithTopicSubscriptionWithNoPartitionsIsSuccessful()
    {
        $topicName = 'test';
        $timeout = 0;
        $partitions = [$this->getMetadataPartitionMock(1), $this->getMetadataPartitionMock(2)];
        $expectedBrokers = ['broker'];

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

        $queueMock = $this->getRdKafkaQueue();

        $queueMock
            ->expects(self::once())
            ->method('consume')
            ->with($timeout)
            ->willReturn($messageMock);

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->getMock();

        $topicMetadataMock = $this->getMockBuilder(Metadata\Topic::class)
            ->disableOriginalConstructor()
            ->setMethods(['getPartitions'])
            ->getMock();

        $topicMetadataMock
            ->expects(self::once())
            ->method('getPartitions')
            ->willReturn($partitions);

        $metadataMock = $this->getMockBuilder(Metadata::class)
            ->disableOriginalConstructor()
            ->setMethods(['getTopics'])
            ->getMock();

        $metadataMock
            ->expects(self::once())
            ->method('getTopics')
            ->willReturnCallback(
                function () use ($topicMetadataMock) {
                    $collection = $this->getMockBuilder(Metadata\Collection::class)
                        ->disableOriginalConstructor()
                        ->setMethods(['current'])
                        ->getMock();

                    $collection
                        ->expects(self::once())
                        ->method('current')
                        ->willReturn($topicMetadataMock);

                    return $collection;
                }
            );

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumerMock
            ->expects(self::any())
            ->method('consume')
            ->willReturn($messageMock);

        $consumerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $topicMock, $timeout)
            ->willReturn($metadataMock);

        $consumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($topicName)
            ->willReturn($topicMock);

        $consumerMock
            ->expects(self::once())
            ->method('addBrokers')
            ->with(implode(',', $expectedBrokers));

        $consumer = new KafkaConsumer($consumerMock, $expectedBrokers, [new TopicSubscription($topicName)], $timeout);

        $consumer->subscribe();

        $message = $consumer->consume();

        self::assertInstanceOf(Message::class, $message);

        self::assertEquals($messageMock->payload, $message->getBody());
        self::assertEquals($messageMock->offset, $message->getOffset());
        self::assertEquals($messageMock->partition, $message->getPartition());
    }

    public function testConsumeThrowsTimeoutExceptionIfQueueConsumeReturnsNull()
    {
        $exceptionMessage = 'Local: Timed out';
        $timeout = 0;

        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionCode(RD_KAFKA_RESP_ERR__TIMED_OUT);
        self::expectExceptionMessage($exceptionMessage);

        $queueMock = $this->getRdKafkaQueue();

        $queueMock
            ->expects(self::once())
            ->method('consume')
            ->with($timeout)
            ->willReturn(null);

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $topicSubscription = new TopicSubscription('test');
        $topicSubscription->addPartition(1);

        $consumer = new KafkaConsumer($consumerMock, [], [], $timeout);

        $consumer->subscribe();

        $consumer->consume();
    }

    public function testConsumeThrowsExceptionIfConsumedMessageHasNoTopicAndErrorCodeIsNotOkay()
    {
        $exceptionMessage = 'Unknown error';
        $timeout = 0;
        $topicName = 'test';
        $partitionId = 1;
        $defaultOffset = 42;

        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage($exceptionMessage);

        /** @var RdKafkaMessage|MockObject $messageMock */
        $messageMock = $this->getMockBuilder(RdKafkaMessage::class)
            ->setMethods(['errstr'])
            ->getMock();

        $messageMock->err = -185;
        $messageMock->partition = 1;
        $messageMock->offset = 42;
        $messageMock->topic_name = null;

        $messageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn($exceptionMessage);

        $queueMock = $this->getRdKafkaQueue();

        $queueMock
            ->expects(self::once())
            ->method('consume')
            ->with($timeout)
            ->willReturn($messageMock);

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['consumeQueueStart'])
            ->getMock();

        $topicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with($partitionId, $defaultOffset, $queueMock)
            ->willReturn(null);

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($topicName)
            ->willReturn($topicMock);

        $topicSubscription = new TopicSubscription($topicName, $defaultOffset);
        $topicSubscription->addPartition($partitionId);

        $consumer = new KafkaConsumer($consumerMock, [], [$topicSubscription], $timeout);

        $consumer->subscribe();

        $consumer->consume();
    }

    public function testConsumeFailThrowsException()
    {
        $exceptionMessage = 'Unknown error';
        $timeout = 0;
        $topicName = 'test';
        $partitionId = 1;
        $defaultOffset = 42;

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

        $queueMock = $this->getRdKafkaQueue();

        $queueMock
            ->expects(self::once())
            ->method('consume')
            ->with($timeout)
            ->willReturn($messageMock);

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['consumeQueueStart'])
            ->getMock();

        $topicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with($partitionId, $defaultOffset, $queueMock)
            ->willReturn(null);

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($topicName)
            ->willReturn($topicMock);

        $topicSubscription = new TopicSubscription($topicName, $defaultOffset);
        $topicSubscription->addPartition($partitionId);

        $consumer = new KafkaConsumer($consumerMock, [], [$topicSubscription], $timeout);

        $consumer->subscribe();

        $consumer->consume();
    }

    public function testConsumeThrowsExceptionIfConsumerIsCurrentlyNotSubscribed()
    {
        $exceptionMessage = 'This consumer is currently not subscribed';

        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage($exceptionMessage);

        $queueMock = $this->getRdKafkaQueue();

        $queueMock
            ->expects(self::never())
            ->method('consume');

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumer = new KafkaConsumer($consumerMock, [], ['test'], 0);

        $consumer->consume();
    }

    public function testGetTopicsReturnsSubscribableTopicsOfConsumerInstance()
    {
        $topics = [new TopicSubscription('test')];

        $consumerMock = $this->getRdKafkaConsumerMock($this->getRdKafkaQueue());

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        self::assertSame($topics, $consumer->getTopicSubscriptions());
    }

    public function testSubscribeStartsConsumingQueueForTopicAndPartition()
    {
        $partitionId = 1;
        $offset = 42;
        $topicName = 'test';

        $topicSubscription = new TopicSubscription($topicName);
        $topicSubscription->addPartition($partitionId, $offset);

        $topics = [$topicSubscription];

        $queueMock = $this->getRdKafkaQueue();

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['consumeQueueStart'])
            ->getMock();

        $topicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with($partitionId, $offset, $queueMock)
            ->willReturn(null);

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($topicName)
            ->willReturn($topicMock);

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        $consumer->subscribe();
    }

    public function testSubscribeEarlyReturnsIfAlreadySubscribed()
    {
        $partitionId = 1;
        $offset = 42;
        $topicName = 'test';

        $topicSubscription = new TopicSubscription($topicName);
        $topicSubscription->addPartition($partitionId, $offset);

        $topics = [$topicSubscription];

        $queueMock = $this->getRdKafkaQueue();

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumerMock
            ->expects(self::never())
            ->method('newTopic');

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        $subcribedProperty = new \ReflectionProperty(KafkaConsumer::class, 'subscribed');
        $subcribedProperty->setAccessible(true);

        $subcribedProperty->setValue($consumer, true);

        $consumer->subscribe();
    }

    public function testSubscribeConvertsExtensionExceptionToLibraryException()
    {
        $exceptionMessage = 'foobar';
        $topicName = 'test';

        self::expectException(KafkaConsumerSubscriptionException::class);
        self::expectExceptionMessage($exceptionMessage);

        $topics = [new TopicSubscription($topicName)];

        $consumerMock = $this->getRdKafkaConsumerMock($this->getRdKafkaQueue());

        $consumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($topicName)
            ->willThrowException(new RdKafkaException($exceptionMessage));

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        $consumer->subscribe($topicName);
    }

    public function testSubscribeUseExistingTopicsForResubscribe()
    {
        $partitionId = 1;
        $offset = 42;
        $topicName = 'test';

        $topicSubscription = new TopicSubscription($topicName);
        $topicSubscription->addPartition($partitionId, $offset);

        $topics = [$topicSubscription];

        $queueMock = $this->getRdKafkaQueue();

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['consumeQueueStart', 'consumeStop'])
            ->getMock();

        $topicMock
            ->expects(self::exactly(2))
            ->method('consumeQueueStart')
            ->with($partitionId, $offset, $queueMock)
            ->willReturn(null);

        $topicMock
            ->expects(self::once())
            ->method('consumeStop')
            ->with($partitionId)
            ->willReturn(null);

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with($topicName)
            ->willReturn($topicMock);

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        $consumer->subscribe();

        self::assertTrue($consumer->isSubscribed());

        $consumer->unsubscribe();

        self::assertFalse($consumer->isSubscribed());

        $consumer->subscribe();
    }

    public function testIsSubscribedReturnsCurrentSubscroptionState()
    {
        $topics = [new TopicSubscription('test')];

        $consumerMock = $this->getRdKafkaConsumerMock($this->getRdKafkaQueue());

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        self::assertFalse($consumer->isSubscribed());
    }

    public function testCommitWithMessageStoresOffsetOfIt()
    {
        $partition = 1;
        $offset = 42;
        $topicName = 'topic';

        $message = new Message('some message', $topicName, $partition, $offset);

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['offsetStore'])
            ->getMock();

        $consumerMock = $this->getRdKafkaConsumerMock($this->getRdKafkaQueue());

        $topicProperty = new \ReflectionProperty(KafkaConsumer::class, 'topics');
        $topicProperty->setAccessible(true);

        $topicMock
            ->expects(self::once())
            ->method('offsetStore')
            ->with($message->getPartition(), $message->getOffset());

        $consumer = new KafkaConsumer($consumerMock, [], [], 0);

        $topicProperty->setValue($consumer, [$topicName => $topicMock]);

        $consumer->commit($message);
    }

    public function testCommitWithInvalidObjectThrowsExceptionAndDoesNotTriggerCommit()
    {
        $topicName = 'topic';

        self::expectException(KafkaConsumerCommitException::class);
        self::expectExceptionMessage(
            'Provided message (index: 0) is not an instance of "Jobcloud\Messaging\Kafka\Consumer\Message"'
        );

        $message = new \stdClass();

        $consumerMock = $this->getRdKafkaConsumerMock($this->getRdKafkaQueue());

        $topicProperty = new \ReflectionProperty(KafkaConsumer::class, 'topics');
        $topicProperty->setAccessible(true);

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['offsetStore'])
            ->getMock();

        $topicMock
            ->expects(self::never())
            ->method('offsetStore');

        $consumer = new KafkaConsumer($consumerMock, [], [], 0);

        $topicProperty->setValue($consumer, [$topicName => $topicMock]);

        $consumer->commit($message);
    }

    public function testUnsubscribeEarlyReturnsIfAlreadyUnsubscribed()
    {
        $partitionId = 1;
        $offset = 42;
        $topicName = 'test';

        $topicSubscription = new TopicSubscription($topicName);
        $topicSubscription->addPartition($partitionId, $offset);

        $topics = [$topicSubscription];

        $queueMock = $this->getRdKafkaQueue();

        $topicMock = $this->getMockBuilder(ConsumerTopic::class)
            ->disableOriginalConstructor()
            ->setMethods(['consumeQueueStart', 'consumeStop'])
            ->getMock();

        $topicMock
            ->expects(self::never())
            ->method('consumeStop');

        $consumerMock = $this->getRdKafkaConsumerMock($queueMock);

        $consumer = new KafkaConsumer($consumerMock, [], $topics, 0);

        self::assertFalse($consumer->isSubscribed());

        $consumer->unsubscribe();

        self::assertFalse($consumer->isSubscribed());
    }

    /**
     * @return RdKafkaQueue|MockObject
     */
    private function getRdKafkaQueue(): RdKafkaQueue
    {
        return $this->getMockBuilder(RdKafkaQueue::class)
            ->disableOriginalConstructor()
            ->setMethods(['consume'])
            ->getMock();
    }

    /**
     * @param RdKafkaQueue $queue
     * @return RdKafkaConsumer|MockObject
     */
    private function getRdKafkaConsumerMock(RdKafkaQueue $queue): RdKafkaConsumer
    {
        /** @var RdKafkaConsumer|MockObject $consumerMock */
        $consumerMock = $this->getMockBuilder(RdKafkaConsumer::class)
            ->setMethods(['consume', 'newQueue', 'newTopic', 'getMetadata', 'addBrokers'])
            ->disableOriginalConstructor()
            ->getMock();

        $consumerMock
            ->expects(self::once())
            ->method('newQueue')
            ->willReturn($queue);

        return $consumerMock;
    }

    /**
     * @param int $partitionId
     * @return Metadata\Partition|MockObject
     */
    private function getMetadataPartitionMock(int $partitionId): Metadata\Partition
    {
        $partitionMock = $this->getMockBuilder(Metadata\Partition::class)
            ->disableOriginalConstructor()
            ->setMethods(['getId'])
            ->getMock();

        $partitionMock
            ->expects(self::once())
            ->method('getId')
            ->willReturn($partitionId);

        return $partitionMock;
    }
}
