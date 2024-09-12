package com.vec.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerAsyncDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger= LoggerFactory.getLogger(ProducerAsyncDemo.class.getName());

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("first-topic", "hello  fff world message from java client");

        // send data - asynchronous

        RecordMetadata metadata;
        for (int i = 1; i <= AppConfigs.numEvents; i++) {
            metadata = producer.send(new ProducerRecord<>(AppConfigs.topicName, "No."+i, "Simple Message-" + i)).get();
            logger.info("Message " + i + " persisted with offset " + metadata.offset()
                    + " and timestamp on " + metadata.timestamp());
        }

        producer.close();

    }
}
