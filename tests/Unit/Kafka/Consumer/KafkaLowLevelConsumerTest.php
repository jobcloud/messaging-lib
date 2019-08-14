<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaLowLevelConsumer;
use Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage;
use Jobcloud\Messaging\Kafka\Consumer\TopicSubscription;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerConsumeException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Consumer as RdKafkaLowLevelConsumer;
use RdKafka\ConsumerTopic as RdKafkaConsumerTopic;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\Metadata as RdKafkaMetadata;
use RdKafka\Metadata\Collection as RdKafkaMetadataCollection;
use RdKafka\Metadata\Partition as RdKafkaMetadataPartition;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;
use RdKafka\Queue as RdKafkaQueue;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\AbstractKafkaConsumer
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaLowLevelConsumer
 */
final class KafkaLowLevelConsumerTest extends TestCase
{

    /** @var RdKafkaQueue|MockObject */
    private $rdKafkaQueueMock;

    /** @var RdKafkaLowLevelConsumer|MockObject */
    private $rdKafkaConsumerMock;

    /** @var KafkaConfiguration|MockObject */
    private $kafkaConfigurationMock;

    /** @var KafkaLowLevelConsumer */
    private $kafkaConsumer;

    /**
     * @return void
     */
    public function setUp(): void
    {
        $this->rdKafkaQueueMock = $this->createMock(RdKafkaQueue::class);
        $this->rdKafkaConsumerMock = $this->createMock(RdKafkaLowLevelConsumer::class);
        $this->rdKafkaConsumerMock
            ->expects(self::atLeastOnce())
            ->method('newQueue')
            ->willReturn($this->rdKafkaQueueMock);
        $this->kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $this->kafkaConfigurationMock->expects(self::any())->method('dump')->willReturn([]);
        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock);
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testConsumeWithTopicSubscriptionWithNoPartitionsIsSuccessful(): void
    {
        $partitions = [
            $this->getMetadataPartitionMock(1),
            $this->getMetadataPartitionMock(2)
        ];

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = RD_KAFKA_RESP_ERR_NO_ERROR;
        $rdKafkaMessageMock->topic_name = 'sample_topic';
        $rdKafkaMessageMock->partition = 0;
        $rdKafkaMessageMock->offset = 1;
        $rdKafkaMessageMock->timestamp = 1;
        $rdKafkaMessageMock->headers = null;
        $rdKafkaMessageMock
            ->expects(self::never())
            ->method('errstr');

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);

        /** @var RdKafkaMetadataTopic|MockObject $rdKafkaMetadataTopicMock */
        $rdKafkaMetadataTopicMock = $this->createMock(RdKafkaMetadataTopic::class);
        $rdKafkaMetadataTopicMock
            ->expects(self::once())
            ->method('getPartitions')
            ->willReturn($partitions);

        /** @var RdKafkaMetadata|MockObject $rdKafkaMetadataMock */
        $rdKafkaMetadataMock = $this->createMock(RdKafkaMetadata::class);
        $rdKafkaMetadataMock
            ->expects(self::once())
            ->method('getTopics')
            ->willReturnCallback(
                function () use ($rdKafkaMetadataTopicMock) {
                    /** @var RdKafkaMetadataCollection|MockObject $collection */
                    $collection = $this->createMock(RdKafkaMetadataCollection::class);
                    $collection
                        ->expects(self::once())
                        ->method('current')
                        ->willReturn($rdKafkaMetadataTopicMock);

                    return $collection;
                }
            );

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(1000)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([new TopicSubscription('test-topic')]);
        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $rdKafkaConsumerTopicMock, 1000)
            ->willReturn($rdKafkaMetadataMock);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $message = $this->kafkaConsumer->consume();

        self::assertInstanceOf(KafkaConsumerMessage::class, $message);

        self::assertEquals($rdKafkaMessageMock->payload, $message->getBody());
        self::assertEquals($rdKafkaMessageMock->offset, $message->getOffset());
        self::assertEquals($rdKafkaMessageMock->partition, $message->getPartition());
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testConsumeThrowsTimeoutExceptionIfQueueConsumeReturnsNull(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage(KafkaConsumerConsumeException::NO_MORE_MESSAGES_EXCEPTION_MESSAGE);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(1000)
            ->willReturn(null);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTimeout')
            ->willReturn(1000);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testConsumeThrowsExceptionIfConsumedMessageHasNoTopicAndErrorCodeIsNotOkay(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('Unknown error');

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = -185;
        $rdKafkaMessageMock->partition = 1;
        $rdKafkaMessageMock->offset = 103;
        $rdKafkaMessageMock->topic_name = null;
        $rdKafkaMessageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn('Unknown error');

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with(1, 103, $this->rdKafkaQueueMock)
            ->willReturn(null);

        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(1000)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testConsumeFailThrowsException(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('Unknown error');

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = -1;
        $rdKafkaMessageMock->partition = 1;
        $rdKafkaMessageMock->offset = 103;
        $rdKafkaMessageMock->topic_name = 'test-topic';
        $rdKafkaMessageMock->timestamp = 1;
        $rdKafkaMessageMock->headers = ['key' => 'value'];
        $rdKafkaMessageMock
            ->expects(self::once())
            ->method('errstr')
            ->willReturn('Unknown error');

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeQueueStart')
            ->with(1, 103, $this->rdKafkaQueueMock)
            ->willReturn(null);

        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(1000)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerConsumeException
     * @return void
     */
    public function testConsumeThrowsExceptionIfConsumerIsCurrentlyNotSubscribed(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('This consumer is currently not subscribed');

        $this->kafkaConsumer->consume();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @throws \ReflectionException
     * @return void
     */
    public function testSubscribeEarlyReturnsIfAlreadySubscribed(): void
    {
        $subscribedProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'subscribed');
        $subscribedProperty->setAccessible(true);
        $subscribedProperty->setValue($this->kafkaConsumer, true);

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testSubscribeConvertsExtensionExceptionToLibraryException(): void
    {
        self::expectException(KafkaConsumerSubscriptionException::class);
        self::expectExceptionMessage('TEST_EXCEPTION_MESSAGE');

        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willThrowException(new RdKafkaException('TEST_EXCEPTION_MESSAGE'));

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function testSubscribeUseExistingTopicsForResubscribe(): void
    {
        $topicSubscription = new TopicSubscription('test-topic', [1], 103);

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::exactly(2))
            ->method('consumeQueueStart')
            ->with(1, 103, $this->rdKafkaQueueMock)
            ->willReturn(null);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeStop')
            ->with(1)
            ->willReturn(null);

        $this->kafkaConfigurationMock
            ->expects(self::exactly(3))
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();

        self::assertTrue($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->unsubscribe();

        self::assertFalse($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @throws KafkaConsumerCommitException
     * @throws \ReflectionException
     * @return void
     */
    public function testCommitWithMessageStoresOffsetOfIt(): void
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

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('offsetStore')
            ->with($message->getPartition(), $message->getOffset());

        $rdKafkaConsumerMockProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'topics');
        $rdKafkaConsumerMockProperty->setAccessible(true);
        $rdKafkaConsumerMockProperty->setValue(
            $this->kafkaConsumer,
            ['test-topic' => $rdKafkaConsumerTopicMock]
        );

        $this->kafkaConsumer->commit($message);
    }

    /**
     * @throws KafkaConsumerCommitException
     * @throws \ReflectionException
     * @return void
     */
    public function testCommitWithInvalidObjectThrowsExceptionAndDoesNotTriggerCommit(): void
    {
        self::expectException(KafkaConsumerCommitException::class);
        self::expectExceptionMessage(
            'Provided message (index: 0) is not an instance of "Jobcloud\Messaging\Kafka\Message\KafkaConsumerMessage"'
        );

        $message = new \stdClass();

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::never())
            ->method('offsetStore');

        $rdKafkaConsumerMockProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'topics');
        $rdKafkaConsumerMockProperty->setAccessible(true);
        $rdKafkaConsumerMockProperty->setValue($this->kafkaConsumer, ['test-topic' => $rdKafkaConsumerTopicMock]);

        $this->kafkaConsumer->commit($message);
    }

    /**
     * @return void
     */
    public function testUnsubscribeEarlyReturnsIfAlreadyUnsubscribed(): void
    {
        self::assertFalse($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->unsubscribe();
    }

    /**
     * @return void
     */
    public function testIsSubscribedReturnsDefaultSubscriptionState(): void
    {
        self::assertFalse($this->kafkaConsumer->isSubscribed());
    }

    /**
     * @return void
     */
    public function testGetConfiguration(): void
    {
        self::assertIsArray($this->kafkaConsumer->getConfiguration());
    }

    /**
     * @return void
     */
    public function testGetFirstOffsetForTopicPartition(): void
    {
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('queryWatermarkOffsets')
            ->with('test-topic', 1, 0, 0, 1000)
            ->willReturnCallback(
                function (string $topic, int $partition, int &$lowOffset, int &$highOffset, int $timeout) {
                    $lowOffset++;
                }
            );

        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock);

        $lowOffset = $this->kafkaConsumer->getFirstOffsetForTopicPartition('test-topic', 1, 1000);

        $this->assertEquals(1, $lowOffset);
    }

    /**
     * @return void
     */
    public function testGetLastOffsetForTopicPartition(): void
    {
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('queryWatermarkOffsets')
            ->with('test-topic', 1, 0, 0, 1000)
            ->willReturnCallback(
                function (string $topic, int $partition, int &$lowOffset, int &$highOffset, int $timeout) {
                    $highOffset += 5;
                }
            );

        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock);

        $lowOffset = $this->kafkaConsumer->getLastOffsetForTopicPartition('test-topic', 1, 1000);

        $this->assertEquals(5, $lowOffset);
    }

    /**
     * @param int $partitionId
     * @return RdKafkaMetadataPartition|MockObject
     */
    private function getMetadataPartitionMock(int $partitionId): RdKafkaMetadataPartition
    {
        $partitionMock = $this->getMockBuilder(RdKafkaMetadataPartition::class)
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
