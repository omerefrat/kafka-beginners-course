package com.github.omerefrat.kafka.tutorial1;
import org.slf4j.LoggerFactory;
import twitter4j.*;

public class BasicTwitterReader {
    public static void main(String[] args){

        final org.slf4j.Logger logger = LoggerFactory.getLogger(BasicTwitterReader.class);

        // The factory instance is re-useable and thread safe.
        Twitter twitter = TwitterFactory.getSingleton();
        Query query = new Query("source: netanyahu");

        try {
            QueryResult result = twitter.search(query);
            for (Status status : result.getTweets()) {
                System.out.println("@" + status.getUser().getScreenName() + ":" + status.getText());
            }
        } catch (TwitterException e){
            logger.error("Error executing Twitter query: ",e.getErrorMessage());
        }


    }
}
