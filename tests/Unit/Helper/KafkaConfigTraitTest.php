<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Helper;

use PHPUnit\Framework\TestCase;
use Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait;
use RdKafka\Conf;


class KafkaConfigTraitTestClass
{
    use KafkaConfigTrait;

    public function createTraitKafkaConfig(array $config)
    {
        return $this->createKafkaConfig($config);
    }
}


/**
 * @covers Jobcloud\Messaging\Kafka\Helper\KafkaConfigTrait
 */
class KafkaConfigTraitTest extends TestCase
{
    public function testCreateKafkaConfig()
    {
        $sot = new KafkaConfigTraitTestClass();

        $this->assertInstanceOf(Conf::class, $sot->createTraitKafkaConfig([]));
    }
}