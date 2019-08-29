<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Exception\IncompatibleAvroSchemaException;
use FlixTech\SchemaRegistryApi\Exception\SchemaRegistryException;
use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Exception\KafkaMessageException;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Message\MessageInterface;
use Jobcloud\Messaging\Kafka\Exception\KafkaProducerException;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

/**
 * @covers \Jobcloud\Messaging\Kafka\Producer\KafkaProducer
 */
class KafkaProducerTest extends TestCase
{

    /** @var KafkaConfiguration|MockObject */
    private $kafkaConfigurationMock;

    /** @var RdKafkaProducer|MockObject */
    private $rdKafkaProducerMock;

    /** @var KafkaProducer */
    private $kafkaProducer;

    public function setUp(): void
    {
        $this->kafkaConfigurationMock = $this->createMock(KafkaConfiguration::class);
        $this->rdKafkaProducerMock = $this->createMock(RdKafkaProducer::class);
        $this->kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock);
    }

    /**
     * @return void
     * @throws SchemaRegistryException
     * @throws KafkaProducerException
     */
    public function testProduceError(): void
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        self::expectException(KafkaProducerException::class);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                0,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            )
            ->willThrowException(new KafkaProducerException());

        $this->rdKafkaProducerMock
            ->expects(self::any())
            ->method('newTopic')
            ->willReturn($rdKafkaProducerTopicMock);

        $this->kafkaProducer->produce($message);
    }

    /**
     * @return void
     * @throws SchemaRegistryException
     * @throws KafkaProducerException
     */
    public function testProduceErrorOnMessageInterface(): void
    {
        self::expectException(KafkaProducerException::class);
        self::expectExceptionMessage(
            sprintf(
                KafkaProducerException::UNSUPPORTED_MESSAGE_EXCEPTION_MESSAGE,
                KafkaProducerMessageInterface::class
            )
        );

        $message = $this->createMock(MessageInterface::class);

        $this->kafkaProducer->produce($message);
    }

    public function testProduceSuccess()
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                0,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            );

        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(3))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                        case 1:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(1000);

        $this->kafkaProducer->produce($message);
    }

    public function testProduceTombstoneSuccess()
    {
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody(null)
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                0,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            );

        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(3))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                        case 1:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(1000);

        $this->kafkaProducer->produce($message);
    }

    public function testProduceSuccessWithRegistryButNoSchema()
    {
        $schemaRegistry = $this->getMockForAbstractClass(Registry::class);
        $kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $schemaRegistry);

        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('some test content')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                0,
                $message->getBody(),
                $message->getKey(),
                $message->getHeaders()
            );

        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(3))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                        case 1:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(1000);

        $kafkaProducer->produce($message);
    }

    public function testProduceSuccessWithRegistryWithSchemaWithoutVersion()
    {
        $avroSchema = \AvroSchema::parse('{
            "type": "record",
            "namespace": "example",
            "name": "Test",
            "fields": [
                { "name": "name", "type": "string" }
            ]
        }');

        $schemaRegistry = $this->getMockForAbstractClass(Registry::class);
        $schemaRegistry->expects(self::once())->method('latestVersion')->willReturn($avroSchema);
        $schemaRegistry->expects(self::exactly(2))->method('schemaId')->willReturn(1);
        $kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $schemaRegistry);
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": "some name"}')
            ->withHeaders([ 'key' => 'value' ]);

        $serializer = new RecordSerializer($schemaRegistry);
        $binaryString = $serializer->encodeRecord(
            'test',
            $avroSchema,
            json_decode($message->getBody(), true)
        );

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                0,
                $binaryString,
                $message->getKey(),
                $message->getHeaders()
            );

        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(3))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                        case 1:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(1000);

        $kafkaProducer->produce($message, 'testSchema');
    }

    public function testProduceThrowsExceptionOnMessageBodyEncode()
    {
        self::expectException(SchemaRegistryException::class);

        $avroSchema = \AvroSchema::parse('{
            "type": "record",
            "namespace": "example",
            "name": "Test",
            "fields": [
                { "name": "name", "type": "string" }
            ]
        }');

        $schemaRegistry = $this->getMockForAbstractClass(Registry::class);
        $schemaRegistry->expects(self::once())->method('latestVersion')->willReturn($avroSchema);
        $schemaRegistry->expects(self::at(0))->method('schemaId')->willReturn(1);
        $schemaRegistry->expects(self::at(1))->method('schemaId')->willThrowException(new IncompatibleAvroSchemaException());
        $kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $schemaRegistry);
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": "some name"}')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock->expects(self::never())->method('producev');

        $this->kafkaConfigurationMock->expects(self::never())->method('getTimeout');
        $this->rdKafkaProducerMock->expects(self::never())->method('getOutQLen');
        $this->rdKafkaProducerMock->expects(self::never())->method('newTopic');
        $this->rdKafkaProducerMock->expects(self::never())->method('poll');

        $kafkaProducer->produce($message, 'testSchema');
    }

    public function testProduceSuccessWithRegistryWithSchema()
    {
        $avroSchema = \AvroSchema::parse('{
            "type": "record",
            "namespace": "example",
            "name": "Test",
            "fields": [
                { "name": "name", "type": "string" }
            ]
        }');

        $schemaRegistry = $this->getMockForAbstractClass(Registry::class);
        $schemaRegistry->expects(self::once())->method('schemaForSubjectAndVersion')->willReturn($avroSchema);
        $recordSerializer = $this->createMock(RecordSerializer::class);
        $recordSerializer->expects(self::once())->method('encodeRecord')->willReturn('test');
        $kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $schemaRegistry);
        $reflectionProperty = new \ReflectionProperty($kafkaProducer, 'recordSerializer');
        $reflectionProperty->setAccessible(true);
        $reflectionProperty->setValue($kafkaProducer, $recordSerializer);
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": "some name"}')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock
            ->expects(self::once())
            ->method('producev')
            ->with(
                $message->getPartition(),
                0,
                'test',
                $message->getKey(),
                $message->getHeaders()
            );

        $this->kafkaConfigurationMock
            ->expects(self::exactly(2))
            ->method('getTimeout')
            ->willReturn(1000);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(3))
            ->method('getOutQLen')
            ->willReturnCallback(
                function () {
                    static $messageCount = 0;
                    switch ($messageCount++) {
                        case 0:
                        case 1:
                            return 1;
                        default:
                            return 0;
                    }
                }
            );
        $this->rdKafkaProducerMock
            ->expects(self::once())
            ->method('newTopic')
            ->with('test-topic')
            ->willReturn($rdKafkaProducerTopicMock);
        $this->rdKafkaProducerMock
            ->expects(self::exactly(2))
            ->method('poll')
            ->with(1000);

        $kafkaProducer->produce($message, 'testSchema', 1);
    }

    public function testProduceThrowsSchemaRegistryException()
    {
        self::expectException(SchemaRegistryException::class);

        $avroSchema = \AvroSchema::parse('{
            "type": "record",
            "namespace": "example",
            "name": "Test",
            "fields": [
                { "name": "name", "type": "string" }
            ]
        }');

        $schemaRegistry = $this->getMockForAbstractClass(Registry::class);
        $schemaRegistry->expects(self::once())->method('latestVersion')->willThrowException(new IncompatibleAvroSchemaException());
        $kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $schemaRegistry);
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": "some name"}')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock->expects(self::never())->method('producev');
        $this->kafkaConfigurationMock->expects(self::never())->method('getTimeout');
        $this->rdKafkaProducerMock->expects(self::never())->method('getOutQLen');
        $this->rdKafkaProducerMock->expects(self::never())->method('newTopic');
        $this->rdKafkaProducerMock->expects(self::never())->method('poll');

        $kafkaProducer->produce($message, 'testSchema');
    }

    public function testProduceThrowsKafkaMessageException()
    {
        self::expectException(KafkaMessageException::class);

        $schemaRegistry = $this->getMockForAbstractClass(Registry::class);
        $kafkaProducer = new KafkaProducer($this->rdKafkaProducerMock, $this->kafkaConfigurationMock, $schemaRegistry);
        $message = KafkaProducerMessage::create('test-topic', 1)
            ->withKey('asdf-asdf-asfd-asdf')
            ->withBody('{"name": some name"}')
            ->withHeaders([ 'key' => 'value' ]);

        /** @var RdKafkaProducerTopic|MockObject $rdKafkaProducerTopicMock */
        $rdKafkaProducerTopicMock = $this->createMock(RdKafkaProducerTopic::class);
        $rdKafkaProducerTopicMock->expects(self::never())->method('producev');
        $this->kafkaConfigurationMock->expects(self::never())->method('getTimeout');
        $this->rdKafkaProducerMock->expects(self::never())->method('getOutQLen');
        $this->rdKafkaProducerMock->expects(self::never())->method('newTopic');
        $this->rdKafkaProducerMock->expects(self::never())->method('poll');

        $kafkaProducer->produce($message, 'testSchema');
    }
}
