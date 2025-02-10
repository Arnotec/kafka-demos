package org.arnotec.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.arnotec.kafka.producers.ProducerDemo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.Properties;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("I am a producer");
//        System.out.println("Hello, World!");

        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerDemo producerDemo = new ProducerDemo();
        producerDemo.setProducer(producer);

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Bonjour " + i;

                // create a Producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producerDemo.getProducer().send(producerRecord,
                        (recordMetadata, e) -> {
                            // executes every time a record successfully sent or an exception is thrown
                            if (e == null) {
                                // the record was successfully sent
                                log.info(MessageFormat.format("Topic: {0} | Key: {1} | Partition: {2} | Timestamp: {3}",
                                        recordMetadata.topic(), producerRecord.key(), recordMetadata.partition(),
                                        Instant.ofEpochMilli(recordMetadata.timestamp())));
                            }
                            else {
                                log.error("Error while producing", e);
                            }
                        });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }


        // tell the producer to send all data and block until done -- synchronous
        producerDemo.getProducer().flush();

        // flush and close the producer
        producerDemo.getProducer().close();




    }
}