package com.github.omerefrat.kafka.tutorial4;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {

    private static Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    public static void main(String[] args) {
        // create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-sreams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_status_connect");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k , jsonTweets) -> getUserFollowersInTweets(jsonTweets) > 10000
        );

        filteredStream.to("important_tweets");

        // build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties);

        // start our streams application
        kafkaStreams.start();
    }

    private static int getUserFollowersInTweets(String jsonString) {

        int followers = 0;

        try{
            JSONObject json = new JSONObject(jsonString);
            JSONObject jsonUser =  json.getJSONObject("payload").getJSONObject("User");
            followers = jsonUser.getInt("FollowersCount");
        } catch (Exception e){
            logger.error("Error extracting user followers from json",e);
        }

        return  followers;
    }
}
