<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

use Jobcloud\Messaging\Consumer\ConsumerException;
use Jobcloud\Messaging\Kafka\Consumer\Message;

class KafkaConsumerConsumeException extends ConsumerException
{

    /**
     * @var Message|null
     */
    private $kafkaMessage;

    /**
     * @param string          $message
     * @param integer         $code
     * @param Message|null    $kafkaMessage
     * @param \Throwable|null $previous
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        Message $kafkaMessage = null,
        \Throwable $previous = null
    ) {
        parent::__construct($message, $code, $previous);

        $this->kafkaMessage = $kafkaMessage;
    }

    /**
     * @return Message
     */
    public function getKafkaMessage(): ?Message
    {
        return $this->kafkaMessage;
    }
}
