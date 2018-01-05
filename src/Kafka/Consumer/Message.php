<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;

class Message implements MessageInterface
{

    /**
     * @var int
     */
    private $errorCode;

    /**
     * @var string
     */
    private $errorMessage;

    /**
     * @var string|null
     */
    private $body;

    /**
     * @var string
     */
    private $topicName;
    
    /**
     * @var int
     */
    private $offset;

    /**
     * @var int
     */
    private $partition;

    /**
     * @param null|string $body
     * @param string      $topicName
     * @param int         $partition
     * @param int         $offset
     * @param int         $errorCode
     * @param string      $errorMessage
     */
    public function __construct(
        ?string $body,
        string $topicName,
        int $partition,
        int $offset,
        int $errorCode,
        ?string $errorMessage
    ) {
        $this->body         = $body;
        $this->topicName    = $topicName;
        $this->partition    = $partition;
        $this->offset       = $offset;
        $this->errorCode    = $errorCode;
        $this->errorMessage = $errorMessage;
    }

    /**
     * @return int
     */
    public function getErrorCode(): int
    {
        return $this->errorCode;
    }

    /**
     * @return string
     */
    public function getErrorMessage(): string
    {
        return $this->errorMessage;
    }

    /**
     * @return null|string
     */
    public function getBody(): ?string
    {
        return $this->body;
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

    /**
     * @return int
     */
    public function getPartition(): int
    {
        return $this->partition;
    }
}
