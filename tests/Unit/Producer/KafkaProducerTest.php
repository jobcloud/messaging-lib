<?php

namespace Jobcloud\Messaging\Tests\Unit\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Producer\KafkaProducer;
use RdKafka\Producer as RdKafkaProducer;
use PHPUnit\Framework\TestCase;
use RdKafka\ProducerTopic;
use RdKafka\Conf;

/**
 * @covers Jobcloud\Messaging\Kafka\Producer\KafkaProducer
 * @covers Jobcloud\Messaging\Kafka\Producer\AbstractKafkaProducer
 *
 */
class KafkaProducerTest extends TestCase
{

    /**
     * @var $producer KafkaProducer
     */
    protected $producer;

    public function setUp()
    {
        $callback = function ($kafka, $errId, $msg) {
            //do nothing
        };

        $conf = new Conf();
        $conf->setErrorCb($callback);
        $conf->setDrMsgCb($callback);
        $producer = new RdKafkaProducer($conf);

        $this->producer = new KafkaProducer($producer, ['localhost']);
    }


    public function testGetProducerTopicForTopic()
    {
        $producerTopic = $this->producer->getProducerTopicForTopic('testTopic');

        $this->assertInstanceOf(ProducerTopic::class, $producerTopic);
    }
}