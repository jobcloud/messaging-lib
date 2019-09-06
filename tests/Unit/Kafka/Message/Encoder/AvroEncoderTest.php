<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message\Encoder;

use FlixTech\SchemaRegistryApi\Registry;
use Jobcloud\Messaging\Kafka\Exception\AvroNormalizerException;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessageInterface;
use Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder;
use Jobcloud\Messaging\Kafka\Message\Transformer\AvroTransformerInterface;
use PHPStan\Testing\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder
 * @covers \Jobcloud\Messaging\Kafka\Message\Helper\SchemaRegistryHelperTrait
 */
class AvroEncoderTest extends TestCase
{
    public function testNormalizeTombstone()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::once())->method('getBody')->willReturn(null);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::never())->method('encodeValue');
        $normalizer = new AvroEncoder($transformer, []);
        $result = $normalizer->encode($producerMessage);

        self::assertSame($producerMessage, $result);
    }

    public function testNormalizeWithoutSchema()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(3))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::once())->method('getBody')->willReturn('test');


        self::expectException(AvroNormalizerException::class);
        self::expectExceptionMessage(
            sprintf(
                AvroNormalizerException::NO_SCHEMA_FOR_TOPIC_MESSAGE,
                $producerMessage->getTopicName()
            )
        );

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $normalizer = new AvroEncoder($transformer, []);
        $normalizer->encode($producerMessage);
    }

    public function testNormalizeWithoutSchemaInterface()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(6))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn('{}');

        self::expectException(AvroNormalizerException::class);
        self::expectExceptionMessage(
            sprintf(
                AvroNormalizerException::WRONG_SCHEMA_MAPPING_TYPE_MESSAGE,
                $producerMessage->getTopicName(),
                KafkaAvroSchemaInterface::class
            )
        );

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $normalizer = new AvroEncoder($transformer, ['test' => 'bla']);
        $normalizer->encode($producerMessage);
    }

    public function testNormalizeWithoutJsonBody()
    {
        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getTopicName')->willReturn('test');
        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        self::expectException(AvroNormalizerException::class);
        self::expectExceptionMessage(AvroNormalizerException::MESSAGE_BODY_MUST_BE_JSON_MESSAGE);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $normalizer = new AvroEncoder($transformer, ['test' => $avroSchema]);
        $normalizer->encode($producerMessage);
    }

    public function testNormalizeSuccessWithoutVersion()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();
        $registry = $this->getMockForAbstractClass(Registry::class);
        $registry->expects(self::once())->method('latestVersion')->willReturn($schemaDefinition);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(4))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn('{}');
        $producerMessage->expects(self::once())->method('withBody')->with('encodedValue')->willReturn($producerMessage);

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(3))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::once())->method('getVersion')->willReturn(null);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::once())->method('getSchemaRegistry')->willReturn($registry);
        $transformer->expects(self::once())->method('encodeValue')->with($avroSchema->getName(), $schemaDefinition, [])->willReturn('encodedValue');
        $normalizer = new AvroEncoder($transformer, ['test' => $avroSchema]);

        self::assertSame($producerMessage, $normalizer->encode($producerMessage));
    }

    public function testNormalizeSuccessWithVersion()
    {
        $schemaDefinition = $this->getMockBuilder(\AvroSchema::class)->disableOriginalConstructor()->getMock();
        $registry = $this->getMockForAbstractClass(Registry::class);
        $registry->expects(self::once())->method('schemaForSubjectAndVersion')->willReturn($schemaDefinition);

        $producerMessage = $this->getMockForAbstractClass(KafkaProducerMessageInterface::class);
        $producerMessage->expects(self::exactly(4))->method('getTopicName')->willReturn('test');
        $producerMessage->expects(self::exactly(2))->method('getBody')->willReturn('{}');
        $producerMessage->expects(self::once())->method('withBody')->with('encodedValue')->willReturn($producerMessage);

        $avroSchema = $this->getMockForAbstractClass(KafkaAvroSchemaInterface::class);
        $avroSchema->expects(self::exactly(3))->method('getName')->willReturn('schemaName');
        $avroSchema->expects(self::exactly(2))->method('getVersion')->willReturn(1);

        $transformer = $this->getMockForAbstractClass(AvroTransformerInterface::class);
        $transformer->expects(self::once())->method('getSchemaRegistry')->willReturn($registry);
        $transformer->expects(self::once())->method('encodeValue')->with($avroSchema->getName(), $schemaDefinition, [])->willReturn('encodedValue');
        $normalizer = new AvroEncoder($transformer, ['test' => $avroSchema]);

        self::assertSame($producerMessage, $normalizer->encode($producerMessage));
    }
}
