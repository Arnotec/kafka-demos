package org.arnotec.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger LOGGER = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        LOGGER.info("Hey! I am a Kafka consumer");

        String topic = "demo_java";
        String groupId = "my-java-application";

        // create consumer properties
        Properties properties = new Properties();

        // set consumer config
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        // property to specifies how to read data by the consumer can be none/earliest/latest
        // none => must set a consumer group. If not, the app will fail
        // earliest => read from the beginning of the topic
        // latest => read data from now
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(List.of(topic));

        // poll for data from the topic
        while (true) {

            LOGGER.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info(MessageFormat.format("Key: {0}, Value: {1}", record.key(), record.value()));
                LOGGER.info(MessageFormat.format("Partition: {0}, Offset: {1}", record.partition(), record.offset()));
            }

        }

    }

}
