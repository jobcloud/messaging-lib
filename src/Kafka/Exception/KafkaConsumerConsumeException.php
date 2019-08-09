<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

use Jobcloud\Messaging\Consumer\ConsumerException;
use Jobcloud\Messaging\Kafka\Message\KafkaMessage;

class KafkaConsumerConsumeException extends ConsumerException
{

    /**
     * @var KafkaMessage|null
     */
    private $kafkaMessage;

    /**
     * @param string            $message
     * @param integer           $code
     * @param KafkaMessage|null $kafkaMessage
     * @param \Throwable|null   $previous
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        KafkaMessage $kafkaMessage = null,
        \Throwable $previous = null
    ) {
        parent::__construct($message, $code, $previous);

        $this->kafkaMessage = $kafkaMessage;
    }

    /**
     * @return Message
     */
    public function getKafkaMessage(): ?KafkaMessage
    {
        return $this->kafkaMessage;
    }
}
