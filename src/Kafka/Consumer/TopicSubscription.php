<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

/**
 * This is partition agnostic topic subscriptions. This means that all the available partitions
 * get subscribed and all start consuming at the same offset.
 */
class TopicSubscription implements TopicSubscriptionInterface
{

    /**
     * @var string
     */
    private $topicName;

    /**
     * @var int
     */
    private $offset;

    /**
     * @param string $topicName
     * @param int    $offset    The offset to start consuming from
     */
    public function __construct(string $topicName, int $offset = RD_KAFKA_OFFSET_STORED)
    {
        $this->topicName = $topicName;
        $this->offset    = $offset;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->topicName;
    }

    /**
     * @return int
     */
    public function getOffset(): int
    {
        return $this->offset;
    }
}
