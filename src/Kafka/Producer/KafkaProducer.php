<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Producer as RdKafkaProducer;
use RdKafka\ProducerTopic as RdKafkaProducerTopic;

final class KafkaProducer implements KafkaProducerInterface
{

    /** @var RdKafkaProducer */
    protected $producer;

    /** @var KafkaConfiguration */
    protected $kafkaConfiguration;

    /** @var array */
    protected $producerTopics = [];

    /**
     * KafkaProducer constructor.
     * @param RdKafkaProducer    $producer
     * @param KafkaConfiguration $kafkaConfiguration
     */
    public function __construct(RdKafkaProducer $producer, KafkaConfiguration $kafkaConfiguration)
    {
        $this->producer = $producer;
        $this->kafkaConfiguration = $kafkaConfiguration;
    }

    /**
     * @param string      $message
     * @param string      $topic
     * @param integer     $partition
     * @param string|null $key
     * @param array|null  $headers
     * @return void
     */
    public function produce(
        string $message,
        string $topic,
        int $partition = RD_KAFKA_PARTITION_UA,
        string $key = null,
        ?array $headers = null
    ) {
        $topicProducer = $this->getProducerTopicForTopic($topic);

        $topicProducer->producev($partition, 0, $message, $key, $headers);

        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll($this->kafkaConfiguration->getTimeout());
        }
    }

    /**
     * @param string $topic
     * @return RdKafkaProducerTopic
     */
    private function getProducerTopicForTopic(string $topic): RdKafkaProducerTopic
    {
        if (!isset($this->producerTopics[$topic])) {
            $this->producerTopics[$topic] = $this->producer->newTopic($topic);
        }

        return $this->producerTopics[$topic];
    }

    /**
     * @param integer $purgeFlags
     * @return integer
     */
    public function purge(int $purgeFlags): int
    {
        return $this->producer->purge($purgeFlags);
    }
}
