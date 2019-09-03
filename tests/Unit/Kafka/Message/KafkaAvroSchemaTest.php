<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

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

        $readerSchema = new KafkaAvroSchema($schemaName, $version);

        self::assertEquals($schemaName, $readerSchema->getSchemaName());
        self::assertEquals($version, $readerSchema->getVersion());
    }

    public function testReaderSchemaWithNoVersion()
    {
        $schemaName = 'testSchema';

        $readerSchema = new KafkaAvroSchema($schemaName);

        self::assertEquals($schemaName, $readerSchema->getSchemaName());
        self::assertNull($readerSchema->getVersion());
    }
}
