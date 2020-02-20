package com.shekhar.kafka.elasticsearch;

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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ElasticsearchConsumer {

    static final Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

    public static RestHighLevelClient createClient() {

        // REPLACE WITH YOURS
        String esAccessUrl = "kafka-course-7008146071.us-east-1.bonsaisearch.net";
        String accessKey = "gro385vmn0";
        String accessSecret = "6wvyrpwh45";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(accessKey, accessSecret));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(esAccessUrl, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;

    }

    public static void main(String[] args) {
        String topic = "movie_topic";

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer(topic);

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record: records) {
                // insert data to elasticsearch

                IndexRequest indexRequest = new IndexRequest("movie", "rating").source(record.value(), XContentType.JSON);

                try {
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info(id);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                } catch (IOException e) {
                    logger.error("error in indexing: ", e);
                }
            }
        }

//        try {
//            client.close();
//        } catch (IOException e) {
//            logger.error("error in closing client: ", e);
//        }

    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String hostUrl = "127.0.0.1:9092";
        String groupId = "movie-app";

        List<String> reset_offsets = (Arrays.asList("earliest","latest","none"));

        // consumer properties
        // check: https://kafka.apache.org/documentation/#consumerconfigs
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, hostUrl);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset_offsets.get(0));

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // subscribe consumer to multiple topics
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

}
