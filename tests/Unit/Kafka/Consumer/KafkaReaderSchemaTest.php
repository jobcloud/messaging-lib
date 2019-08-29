<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Consumer;

use Jobcloud\Messaging\Kafka\Consumer\KafkaReaderSchema;
use PHPUnit\Framework\TestCase;

/**
 * @covers \Jobcloud\Messaging\Kafka\Consumer\KafkaReaderSchema
 */
class KafkaReaderSchemaTest extends TestCase
{
    public function testGettersAndSetters()
    {
        $schemaName = 'testSchema';
        $version = 9;

        $readerSchema = new KafkaReaderSchema($schemaName, $version);

        self::assertEquals($schemaName, $readerSchema->getSchemaName());
        self::assertEquals($version, $readerSchema->getVersion());
    }

    public function testReaderSchemaWithNoVersion()
    {
        $schemaName = 'testSchema';

        $readerSchema = new KafkaReaderSchema($schemaName);

        self::assertEquals($schemaName, $readerSchema->getSchemaName());
        self::assertNull($readerSchema->getVersion());
    }
}
