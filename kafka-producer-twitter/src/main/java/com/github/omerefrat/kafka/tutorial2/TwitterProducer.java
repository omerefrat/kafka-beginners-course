package com.github.omerefrat.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.JSONObject;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private String consumerKey = "ZEEXhF5FsgsM05e5rE02erI3r";
    private String consumerSecret = "WbicchxPuqEiWLUrXL34wRmPmXotIVeXT6ZU09Hxpt7Myma8l9";
    private String token = "54547796-nC0anOMro8P2BilV6vMESnwN8yBW5FTXRo0AJEqhI";
    private String secret = "KhTbrFR3RgbWG3nVF5LfQB1Uoi2X0DVQVZcjx05guVyQI";

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer(){
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){

        logger.info("Setup");

        // create a Twitter client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Client twitterClient = createTwitterClient(msgQueue);
        twitterClient.connect();

        // get the Producer
        String topic = "twitter-tweets";
        KafkaProducer<String,String> producer = getKafkaProducer();

        // loop to send tweets to Kafka
        while (!twitterClient.isDone()) {
            String msg = "";
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e){
                e.printStackTrace();
                twitterClient.stop();
            }
            if (msg != null){
                logger.info(msg);

                // parsing the message
                String parsedtMsg = ParseTwitterMessage(msg);
                SendTweetToKafka(producer, topic, parsedtMsg);
            }
        }
        logger.info("End of application");
    }

    private String ParseTwitterMessage(String msg) {

        JSONObject json = new JSONObject(msg);
        JSONObject shortMsg = new JSONObject();

        try {
            shortMsg.append("id", json.getString("id"));
            shortMsg.append("text", json.getString("text"));
            shortMsg.append("created_at", json.getString("created_at"));
            shortMsg.append("retweeted", json.getString("retweeted"));

            JSONObject userJson = json.getJSONObject("user");
            shortMsg.append("screen_name",userJson.getString("screen_name"));
            shortMsg.append("location", userJson.getString("location"));

        } catch (Exception e) {
            logger.error("Error parsing json",e);
        }
        return shortMsg.toString();
    }

    private void SendTweetToKafka(KafkaProducer<String,String> producer, String topic, String msg) {
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic,msg);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("\nReceived new metadata: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });
    }

    private  KafkaProducer<String,String> getKafkaProducer(){

        // create a Kafka producer
        // create Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        // high throughput settings
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString( 32*1024)); // 32K

        // create the Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        return producer;
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("israel");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(
                this.consumerKey,
                this.consumerSecret,
                this.token,
                this.secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));                       // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
}
