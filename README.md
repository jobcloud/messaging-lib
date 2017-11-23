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

$producer = KafkaConsumerBuilder::create()
    ->addBroker('10.0.2.2')
    ->subscribeTopic('test')
    ->build();

while (true) {
    try {
        $message = $producer->consume();
    } catch (ConsumerExcpetion $e) {
        // Failed
    } 
}
```