<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

interface TopicSubscriptionBuilderInterface
{
    /**
     * @return TopicSubscription
     */
    public function build(): TopicSubscription;
}
