package io.arnotec.kafka;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements BackgroundEventHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // nothing here
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) {
        String data = messageEvent.getData();
        LOGGER.info(data);
        // asynchronous
        kafkaProducer.send(new ProducerRecord<>(topic, data));
    }

    @Override
    public void onComment(String s) throws Exception {
        // nothing here
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("Error in Stream Reading", throwable);
    }
}
