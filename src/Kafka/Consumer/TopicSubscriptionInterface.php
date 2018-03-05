<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use RdKafka\TopicConf;

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
     * @return integer
     */
    public function getDefaultOffset(): int;

    /**
     * @return TopicConf
     */
    public function getTopicConf(): TopicConf;
}
