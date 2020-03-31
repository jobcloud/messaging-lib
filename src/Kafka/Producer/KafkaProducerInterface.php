<?php

namespace Jobcloud\Messaging\Kafka\Producer;

use Jobcloud\Messaging\Producer\ProducerInterface;
use RdKafka\Metadata\Topic as RdKafkaMetadataTopic;

interface KafkaProducerInterface extends ProducerInterface
{

    /**
     * Purge producer messages that are in flight
     *
     * @param integer $purgeFlags
     * @return integer
     */
    public function purge(int $purgeFlags): int;

    /**
     * Wait until all outstanding produce requests are completed
     *
     * @param integer $timeout
     * @return integer
     */
    public function flush(int $timeout): int;

    /**
     * Queries the broker for metadata on a certain topic
     *
     * @param string $topicName
     * @return RdKafkaMetadataTopic
     */
    public function getMetadataForTopic(string $topicName): RdKafkaMetadataTopic;
}
