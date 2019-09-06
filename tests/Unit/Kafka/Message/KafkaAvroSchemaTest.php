<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Message;

use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema
 */
class KafkaAvroSchemaTest extends TestCase
{
    public function testGettersAndSetters()
    {
        $schemaName = 'testSchema';
        $version = 9;

        $avroSchema = new KafkaAvroSchema($schemaName, $version);

        self::assertEquals($schemaName, $avroSchema->getName());
        self::assertEquals($version, $avroSchema->getVersion());
    }

    public function testAvroSchemaWithNoVersion()
    {
        $schemaName = 'testSchema';

        $avroSchema = new KafkaAvroSchema($schemaName);

        self::assertEquals($schemaName, $avroSchema->getName());
        self::assertNull($avroSchema->getVersion());
    }
}
