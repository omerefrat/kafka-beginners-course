package com.github.omerefrat.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        System.out.println("Hello Omer!");

        final Logger logger;
        logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

        String bootstrapServers = "127.0.0.1:9092";
        //String groupId = "my-seven-application";
        String topic = "first_topic";

        // create Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe the consumer to our topic(s)
        //consumer.subscribe(Collections.singleton(topic));

        // assign and seek
        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading){
            ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record :records){
                numberOfMessagesReadSoFar++;
                logger.info("key: " +  record.key() + ", ");
                logger.info("value: " +  record.value() + ", ");
                logger.info("partition: " +  record.partition() + ", ");
                logger.info("offset: " +  record.offset() + "\n");

                if (numberOfMessagesReadSoFar == numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("Existing the application");
    }
}
