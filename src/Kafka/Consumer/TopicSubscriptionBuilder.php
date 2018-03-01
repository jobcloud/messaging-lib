<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

final class TopicSubscriptionBuilder implements TopicSubscriptionBuilderInterface
{

    /**
     * @var string
     */
    private $topicName;

    /**
     * @var integer
     */
    private $defaultOffset;

    /**
     * TopicSubscriptionBuilder constructor
     */
    private function __construct()
    {
        $this->defaultOffset = RD_KAFKA_OFFSET_STORED;
    }

    /**
     * @return TopicSubscriptionBuilder
     */
    public static function create(): self
    {
        return new self();
    }

    /**
     * @param string $topicName
     * @return TopicSubscriptionBuilder
     */
    public function setTopicName(string $topicName): self
    {
        $this->topicName = $topicName;

        return $this;
    }

    /**
     * @param integer $defaultOffset
     * @return TopicSubscriptionBuilder
     */
    public function setDefaultOffset(int $defaultOffset): self
    {
        $this->defaultOffset = $defaultOffset;

        return $this;
    }

    /**
     * @return TopicSubscription
     */
    public function build(): TopicSubscription
    {
        return new TopicSubscription($this->topicName, $this->defaultOffset);
    }
}
