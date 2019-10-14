package com.github.omerefrat.kafka.tutorial3;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.JSONObject;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasicSearchConsumer {

    static Logger logger = LoggerFactory.getLogger(ElasicSearchConsumer.class.getName());


    public static RestHighLevelClient createClient() {

        String hostname = "omer-test-cluster-8511842564.ap-southeast-2.bonsaisearch.net";
        String username = "5hv9s67u3u";
        String password = "m5gksqcfuy";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    private static KafkaConsumer<String,String>  getKafkaConsumer(){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        String topic = "twitter-tweets";

        // create Consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); // disable auto commit of offsets

        // create the Consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe the consumer to our topic(s)
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        // setup the kafka consumer
        KafkaConsumer<String,String> kafkaConsumer = getKafkaConsumer();

        // pulling the data from kafka
        int numberOfMessagesToRead = 5000;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        // poll for new data
        while (keepOnReading){

            logger.info("reading tweets from kafka...");
            ConsumerRecords<String,String> records =  kafkaConsumer.poll(Duration.ofMillis(100));
            logger.info(records.count() + " new tweets received");

            BulkRequest bulkRequest = new BulkRequest();

            for (ConsumerRecord<String,String> record :records){
                numberOfMessagesReadSoFar++;
                String jsonString = record.value();

                if (!jsonString.isEmpty()){

                    // twitter id
                    String id = getTweetId(jsonString);

                    if (id.isEmpty()) {
                        // create kafka generic id
                        id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    }

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id)
                            .source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest);
                }

                if (numberOfMessagesReadSoFar == numberOfMessagesToRead){
                    keepOnReading = false;
                    break;
                }
            }

            if (records.count() > 0) {

                // save to ES
                logger.info("saving bulk tweets to Elastic Search...");
                BulkResponse bulkResponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);

                logger.info("committing the offset...");
                kafkaConsumer.commitSync();
                logger.info("offset have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        logger.info("Existing the application... " + numberOfMessagesReadSoFar + " messages have been read");
    }

    private static String getTweetId(String jsonString) {

        String id = "";

        try{
            JSONObject json = new JSONObject(jsonString);
            id = json.getString("id_str");
        } catch (Exception e){
            logger.error("Error extracting tweet id (id_str) from json",e);
        }

        return  id;
    }
}
