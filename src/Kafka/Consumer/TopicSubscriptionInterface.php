<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface TopicSubscriptionInterface
{

    /**
     * @return string
     */
    public function getTopicName(): string;

    /**
     * @return array
     */
    public function getPartitions(): array;

    /**
     * @return int
     */
    public function getOffset(): int;
}
