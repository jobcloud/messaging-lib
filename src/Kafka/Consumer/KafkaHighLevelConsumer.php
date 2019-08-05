<?php

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;
use Jobcloud\Messaging\Kafka\Conf\KafkaConfiguration;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerAssignmentException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerCommitException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerRequestException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerSubscriptionException;
use RdKafka\Exception as RdKafkaException;
use RdKafka\Message as RdKafkaMessage;
use RdKafka\TopicPartition;
use RdKafka\KafkaConsumer as RdKafkaHighLevelConsumer;

final class KafkaHighLevelConsumer extends AbstractKafkaConsumer implements KafkaHighLevelConsumerInterface
{

    /** @var RdKafkaHighLevelConsumer */
    protected $consumer;

    /**
     * @param RdKafkaHighLevelConsumer $consumer
     * @param KafkaConfiguration       $kafkaConfiguration
     */
    public function __construct(RdKafkaHighLevelConsumer $consumer, KafkaConfiguration $kafkaConfiguration)
    {
        $this->consumer = $consumer;
        $this->kafkaConfiguration = $kafkaConfiguration;
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function subscribe(): void
    {
        if (true === $this->isSubscribed()) {
            return;
        }

        try {
            $this->consumer->subscribe($this->getConfiguration()->getTopicSubscriptions());
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @throws KafkaConsumerSubscriptionException
     * @return void
     */
    public function unsubscribe(): void
    {
        try {
            $this->consumer->unsubscribe();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerSubscriptionException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param MessageInterface|MessageInterface[] $messages
     * @throws KafkaConsumerCommitException
     * @return void
     */
    public function commit($messages): void
    {
        $this->commitMessages($messages);
    }

    /**
     * @param array $topicPartitions
     * @throws KafkaConsumerAssignmentException
     * @return void
     */
    public function assign(array $topicPartitions): void
    {
        try {
            $this->consumer->assign($topicPartitions);
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerAssignmentException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * @param MessageInterface|MessageInterface[] $messages
     * @throws KafkaConsumerCommitException
     * @return void
     */
    public function commitAsync($messages): void
    {
        $this->commitMessages($messages, true);
    }

    /**
     * @return array
     * @throws KafkaConsumerAssignmentException
     */
    public function getAssignment(): array
    {
        try {
            return $this->consumer->getAssignment();
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerAssignmentException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * @param array|TopicPartition[] $topicPartitions
     * @param integer                $timeout
     * @return array|TopicPartition[]
     * @throws KafkaConsumerRequestException
     */
    public function getCommittedOffsets(array $topicPartitions, int $timeout): array
    {
        try {
            return $this->consumer->getCommittedOffsets($topicPartitions, $timeout);
        } catch (RdKafkaException $e) {
            throw new KafkaConsumerRequestException($e->getMessage(), $e->getCode());
        }
    }

    /**
     * @param integer $timeout
     * @return RdKafkaMessage|null
     * @throws RdKafkaException
     */
    protected function kafkaConsume(int $timeout): ?RdKafkaMessage
    {
        return $this->consumer->consume($timeout);
    }

    /**
     * @param MessageInterface|MessageInterface[] $messages
     * @param boolean                             $asAsync
     * @throws KafkaConsumerCommitException
     * @return void
     */
    private function commitMessages($messages, bool $asAsync = false): void
    {
        $messages = is_array($messages) ? $messages : [$messages];

        foreach ($messages as $i => $message) {
            if (false === $message instanceof Message) {
                throw new KafkaConsumerCommitException(
                    sprintf('Provided message (index: %d) is not an instance of "%s"', $i, Message::class)
                );
            }

            try {
                if (true === $asAsync) {
                    $this->consumer->commitAsync($message);
                } else {
                    $this->consumer->commit($message);
                }
            } catch (RdKafkaException $e) {
                throw new KafkaConsumerCommitException($e->getMessage(), $e->getCode());
            }
        }
    }
}
