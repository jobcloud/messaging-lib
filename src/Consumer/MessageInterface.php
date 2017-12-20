<?php

declare(strict_types=1);

namespace Jobcloud\Messaging\Consumer;

interface MessageInterface
{
    public function getBody(): ?string;
}
