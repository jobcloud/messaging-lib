<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Kafka\Consumer;

use Jobcloud\Messaging\Consumer\MessageInterface;

class Message implements MessageInterface
{

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
     * @param integer     $partition
     * @param integer     $offset
     */
    public function __construct(
        ?string $body,
        string $topicName,
        int $partition,
        int $offset
    ) {
        $this->body         = $body;
        $this->topicName    = $topicName;
        $this->partition    = $partition;
        $this->offset       = $offset;
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
     * @return integer
     */
    public function getOffset(): int
    {
        return $this->offset;
    }

    /**
     * @return integer
     */
    public function getPartition(): int
    {
        return $this->partition;
    }
}
