# messaging-lib

## Usage

### Producer

#### Kafka

```php
<?php

use \Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;

$producer = KafkaProducerBuilder::create()
    ->addBroker('10.0.2.2')
    ->setTopic('test')
    ->build();

$producer->produce('hello world');
```

### Consumer

#### Kafka

```php
<?php

use \Jobcloud\Messaging\Kafka\Producer\KafkaConsumerBuilder;

$consumer = KafkaConsumerBuilder::create()
    ->addBroker('10.0.2.2')
    ->subscribeTopic('test')
    ->build();

while (true) {
    try {
        $message = $consumer->consume();
    } catch (ConsumerExcpetion $e) {
        // Failed
    } 
}
```

### ProducerPool

```php
<?php

use \Jobcloud\Messaging\Producer\ProducerPool;
use \Jobcloud\Messaging\Producer\ProducerInterface;

/** @var ProducerInterface $someKafkaProducer */
/** @var ProducerInterface $someRabbitMQProducer */

$pool = new ProducerPool();
$pool
    ->addProducer($someKafkaProducer)
    ->addProducer($someRabbitMQProducer)
;

$pool->produce('hello world');
```