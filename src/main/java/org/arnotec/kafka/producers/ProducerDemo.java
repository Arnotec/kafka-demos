package org.arnotec.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    private KafkaProducer<String, String> producer;

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }
}
