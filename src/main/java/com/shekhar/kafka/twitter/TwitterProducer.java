package com.shekhar.kafka.twitter;

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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    static final String consumerKey = "pJWsJYxMrOSrmGgQW7qesPw3m";
    static final String consumerSecret = "JUjU1jzpROHW0zyvjZeo8gR7FsaWLDUROljX86bZfo9BD50j72";
    static final String token = "51186919-AfFpKgoRR5hjmO7uepd3E9jSsFHyPZqZqdK8MWk93";
    static final String secret = "nWCzG7ASVdzwuk7nABpUlqbiVXZG9PG2k8nDGDSXlvT0g";

    static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer(){}

    public static void main(String[] args) {

        new TwitterProducer().run();



    }

    public void run() {

        logger.info("setup");

        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();


        // create a kafka producer
        String topic = "tweet_topic";
        KafkaProducer<String, String> producer = createProducer();

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            logger.info(msg);
            producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("something bad happended.", e);
                    }
                }
            });
        }
        logger.info("end");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts host = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka");
        endpoint.trackTerms(terms);

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token,secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Client-01")
                .hosts(host)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client = builder.build();
        return client;

    }

    public KafkaProducer<String, String> createProducer() {
        String hostUrl = "localhost:9092";

        // producer properties
        // check: https://kafka.apache.org/documentation/#producerconfigs
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostUrl);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        return producer;
    }

}
