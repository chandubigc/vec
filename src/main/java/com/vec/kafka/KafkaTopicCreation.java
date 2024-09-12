package com.vec.kafka;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaTopicCreation {

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        AdminClient adminClient = AdminClient.create(properties);
            NewTopic newTopic = new NewTopic("topic-from-java-two", 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)
        NewTopic newTopic3 = new NewTopic("topic-from-java-three", 1, (short)1); //new NewTopic(topicName, numPartitions, replicationFactor)

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);
        newTopics.add(newTopic3);
        adminClient.createTopics(newTopics);
        adminClient.close();
    }
}
