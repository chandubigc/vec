package com.vec.kafka;
import org.apache.kafka.clients.admin.AdminClient;

import org.apache.kafka.clients.admin.ListTopicsResult;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;


import java.io.IOException;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class DeleteKafkaTopics {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        AdminClient adminClient = AdminClient.create(properties);

        Set<String> topics = new HashSet<>();
        topics.add("topic-from-java");
        adminClient.deleteTopics(topics);
        adminClient.close();
    }
}
