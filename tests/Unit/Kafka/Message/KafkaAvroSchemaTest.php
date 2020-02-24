<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message;

use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchemaInterface;
use PHPUnit\Framework\TestCase;
use \AvroSchema;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema
 */
class KafkaAvroSchemaTest extends TestCase
{
    public function testGetters()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $schemaName = 'testSchema';
        $version = 9;

        $avroSchema = new KafkaAvroSchema($schemaName, $version, $definition);

        self::assertEquals($schemaName, $avroSchema->getName());
        self::assertEquals($version, $avroSchema->getVersion());
        self::assertEquals($definition, $avroSchema->getDefinition());
    }

    public function testSetters()
    {
        $definition = $this->getMockBuilder(AvroSchema::class)->disableOriginalConstructor()->getMock();

        $schemaName = 'testSchema';

        $avroSchema = new KafkaAvroSchema($schemaName);

        $avroSchema->setDefinition($definition);

        self::assertEquals($definition, $avroSchema->getDefinition());
    }

    public function testAvroSchemaWithJustName()
    {
        $schemaName = 'testSchema';

        $avroSchema = new KafkaAvroSchema($schemaName);

        self::assertEquals($schemaName, $avroSchema->getName());
        self::assertEquals(KafkaAvroSchemaInterface::LATEST_VERSION, $avroSchema->getVersion());
        self::assertNull($avroSchema->getDefinition());
    }
}
