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

        $config = $sot->createTraitKafkaConfig(['group.id' => 'testGroup']);
        $this->assertInstanceOf(Conf::class, $config);

        $configArray = $config->dump();
        $this->assertTrue(isset($configArray['group.id']));
        $this->assertEquals($configArray['group.id'], 'testGroup');
    }
}