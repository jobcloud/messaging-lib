<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Exception;

use Jobcloud\Messaging\Consumer\ConsumerException;
use Jobcloud\Messaging\Kafka\Message\KafkaMessage;
use Jobcloud\Messaging\Kafka\Message\KafkaMessageInterface;

class KafkaConsumerConsumeException extends ConsumerException
{

    const NOT_SUBSCRIBED_EXCEPTION_MESSAGE = 'This consumer is currently not subscribed';

    /**
     * @var KafkaMessageInterface|null
     */
    private $kafkaMessage;

    /**
     * @param string                     $message
     * @param integer                    $code
     * @param KafkaMessageInterface|null $kafkaMessage
     * @param \Throwable|null            $previous
     */
    public function __construct(
        string $message = '',
        int $code = 0,
        KafkaMessageInterface $kafkaMessage = null,
        \Throwable $previous = null
    ) {
        parent::__construct($message, $code, $previous);

        $this->kafkaMessage = $kafkaMessage;
    }

    /**
     * @return null|KafkaMessageInterface
     */
    public function getKafkaMessage(): ?KafkaMessageInterface
    {
        return $this->kafkaMessage;
    }
}
