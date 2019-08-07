<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaLowLevelConsumer;
use Jobcloud\Messaging\Kafka\Consumer\Message;
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

    /** @var string */
    private const TEST_BROKER = 'TEST_BROKER';
    /** @var string */
    private const TEST_TOPIC = 'TEST_TOPIC';
    /** @var int */
    private const TEST_OFFSET = 99;
    /** @var int */
    private const TEST_TIMEOUT = 9999;
    /** @var string */
    private const TEST_CONSUMER_GROUP = 'TEST_CONSUMER_GROUP';
    /** @var int */
    private const TEST_PARTITION_ID_1 = 1;
    /** @var int */
    private const TEST_PARTITION_ID_2 = 2;

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
            ->expects(self::once())
            ->method('newQueue')
            ->willReturn($this->rdKafkaQueueMock);
        $this->kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $this->kafkaConsumer = new KafkaLowLevelConsumer($this->rdKafkaConsumerMock, $this->kafkaConfigurationMock);
    }

    /**
     * @return void
     */
    public function testConsumeWithTopicSubscriptionWithNoPartitionsIsSuccessful(): void
    {
        $partitions = [
            $this->getMetadataPartitionMock(self::TEST_PARTITION_ID_1),
            $this->getMetadataPartitionMock(self::TEST_PARTITION_ID_2)
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
            ->with(self::TEST_TIMEOUT)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([new TopicSubscription(self::TEST_TOPIC)]);
        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(self::TEST_TIMEOUT);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('getMetadata')
            ->with(false, $rdKafkaConsumerTopicMock, self::TEST_TIMEOUT)
            ->willReturn($rdKafkaMetadataMock);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with(self::TEST_TOPIC)
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $message = $this->kafkaConsumer->consume();

        self::assertInstanceOf(Message::class, $message);

        self::assertEquals($rdKafkaMessageMock->payload, $message->getBody());
        self::assertEquals($rdKafkaMessageMock->offset, $message->getOffset());
        self::assertEquals($rdKafkaMessageMock->partition, $message->getPartition());
    }

    /**
     * @return void
     */
    public function testConsumeThrowsTimeoutExceptionIfQueueConsumeReturnsNull(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionCode(RD_KAFKA_RESP_ERR__TIMED_OUT);
        self::expectExceptionMessage('Local: Timed out');

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(self::TEST_TIMEOUT)
            ->willReturn(null);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTimeout')
            ->willReturn(self::TEST_TIMEOUT);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @return void
     */
    public function testConsumeThrowsExceptionIfConsumedMessageHasNoTopicAndErrorCodeIsNotOkay(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('Unknown error');

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = -185;
        $rdKafkaMessageMock->partition = self::TEST_PARTITION_ID_1;
        $rdKafkaMessageMock->offset = self::TEST_OFFSET;
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
            ->with(self::TEST_PARTITION_ID_1, self::TEST_OFFSET, $this->rdKafkaQueueMock)
            ->willReturn(null);

        $topicSubscription = new TopicSubscription(self::TEST_TOPIC, self::TEST_OFFSET);
        $topicSubscription->addPartition(self::TEST_PARTITION_ID_1);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(self::TEST_TIMEOUT)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTimeout')
            ->willReturn(self::TEST_TIMEOUT);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with(self::TEST_TOPIC)
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @return void
     */
    public function testConsumeFailThrowsException(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('Unknown error');

        /** @var RdKafkaMessage|MockObject $rdKafkaMessageMock */
        $rdKafkaMessageMock = $this->createMock(RdKafkaMessage::class);
        $rdKafkaMessageMock->err = -1;
        $rdKafkaMessageMock->partition = self::TEST_PARTITION_ID_1;
        $rdKafkaMessageMock->offset = self::TEST_OFFSET;
        $rdKafkaMessageMock->topic_name = self::TEST_TOPIC;
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
            ->with(self::TEST_PARTITION_ID_1, self::TEST_OFFSET, $this->rdKafkaQueueMock)
            ->willReturn(null);

        $topicSubscription = new TopicSubscription(self::TEST_TOPIC, self::TEST_OFFSET);
        $topicSubscription->addPartition(self::TEST_PARTITION_ID_1);

        $this->rdKafkaQueueMock
            ->expects(self::once())
            ->method('consume')
            ->with(self::TEST_TIMEOUT)
            ->willReturn($rdKafkaMessageMock);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTimeout')
            ->willReturn(self::TEST_TIMEOUT);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with(self::TEST_TOPIC)
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();
        $this->kafkaConsumer->consume();
    }

    /**
     * @return void
     */
    public function testConsumeThrowsExceptionIfConsumerIsCurrentlyNotSubscribed(): void
    {
        self::expectException(KafkaConsumerConsumeException::class);
        self::expectExceptionMessage('This consumer is currently not subscribed');

        $this->kafkaConsumer->consume();
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testSubscribeEarlyReturnsIfAlreadySubscribed(): void
    {
        $subscribedProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'subscribed');
        $subscribedProperty->setAccessible(true);
        $subscribedProperty->setValue($this->kafkaConsumer, true);

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @return void
     */
    public function testSubscribeConvertsExtensionExceptionToLibraryException(): void
    {
        self::expectException(KafkaConsumerSubscriptionException::class);
        self::expectExceptionMessage('TEST_EXCEPTION_MESSAGE');

        $topicSubscription = new TopicSubscription(self::TEST_TOPIC, self::TEST_OFFSET);
        $topicSubscription->addPartition(self::TEST_PARTITION_ID_1);

        $this->kafkaConfigurationMock
            ->expects(self::once())
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with(self::TEST_TOPIC)
            ->willThrowException(new RdKafkaException('TEST_EXCEPTION_MESSAGE'));

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @return void
     */
    public function testSubscribeUseExistingTopicsForResubscribe(): void
    {
        $topicSubscription = new TopicSubscription(self::TEST_TOPIC);
        $topicSubscription->addPartition(self::TEST_PARTITION_ID_1, self::TEST_OFFSET);

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::exactly(2))
            ->method('consumeQueueStart')
            ->with(self::TEST_PARTITION_ID_1, self::TEST_OFFSET, $this->rdKafkaQueueMock)
            ->willReturn(null);
        $rdKafkaConsumerTopicMock
            ->expects(self::once())
            ->method('consumeStop')
            ->with(self::TEST_PARTITION_ID_1)
            ->willReturn(null);

        $this->kafkaConfigurationMock
            ->expects(self::exactly(3))
            ->method('getTopicSubscriptions')
            ->willReturn([$topicSubscription]);
        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with(self::TEST_TOPIC)
            ->willReturn($rdKafkaConsumerTopicMock);

        $this->kafkaConsumer->subscribe();

        self::assertTrue($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->unsubscribe();

        self::assertFalse($this->kafkaConsumer->isSubscribed());

        $this->kafkaConsumer->subscribe();
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testCommitWithMessageStoresOffsetOfIt(): void
    {
        $message = new Message(
            'some key',
            'some message',
            self::TEST_TOPIC,
            self::TEST_PARTITION_ID_1,
            self::TEST_OFFSET,
            1562324233704,
            null
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
            [self::TEST_TOPIC => $rdKafkaConsumerTopicMock]
        );

        $this->kafkaConsumer->commit($message);
    }

    /**
     * @return void
     * @throws \ReflectionException
     */
    public function testCommitWithInvalidObjectThrowsExceptionAndDoesNotTriggerCommit(): void
    {
        self::expectException(KafkaConsumerCommitException::class);
        self::expectExceptionMessage(
            'Provided message (index: 0) is not an instance of "Jobcloud\Messaging\Kafka\Consumer\Message"'
        );

        $message = new \stdClass();

        /** @var RdKafkaConsumerTopic|MockObject $rdKafkaConsumerTopicMock */
        $rdKafkaConsumerTopicMock = $this->createMock(RdKafkaConsumerTopic::class);
        $rdKafkaConsumerTopicMock
            ->expects(self::never())
            ->method('offsetStore');

        $rdKafkaConsumerMockProperty = new \ReflectionProperty(KafkaLowLevelConsumer::class, 'topics');
        $rdKafkaConsumerMockProperty->setAccessible(true);
        $rdKafkaConsumerMockProperty->setValue($this->kafkaConsumer, [self::TEST_TOPIC => $rdKafkaConsumerTopicMock]);

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
        self::assertEquals($this->kafkaConfigurationMock, $this->kafkaConsumer->getConfiguration());
    }

    /**
     * @return void
     */
    public function testGetBrokerHighLowOffsets(): void
    {
        $lowOffset = 0;
        $highOffset = 0;

        $this->rdKafkaConsumerMock
            ->expects(self::once())
            ->method('queryWatermarkOffsets')
            ->with(self::TEST_TOPIC, self::TEST_PARTITION_ID_1, $lowOffset, $highOffset, self::TEST_TIMEOUT);

        $this->kafkaConsumer->getBrokerHighLowOffsets(self::TEST_TOPIC, self::TEST_PARTITION_ID_1, $lowOffset, $highOffset, self::TEST_TIMEOUT);
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
