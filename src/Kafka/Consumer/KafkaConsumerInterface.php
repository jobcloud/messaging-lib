<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\ConsumerInterface;

interface KafkaConsumerInterface extends ConsumerInterface
{
    /**
     * Tries to subscribe to the given topics and returns a list of successfully subscribed topics
     * @return void
     */
    public function subscribe(): void;

    /**
     * Unsubscribes this consumer from all currently subscribed topics
     * @return void
     */
    public function unsubscribe(): void;

    /**
     * @return boolean
     */
    public function isSubscribed(): bool;
}
