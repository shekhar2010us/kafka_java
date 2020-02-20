package com.shekhar.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
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

public class ElasticsearchTest {

    static final Logger logger = LoggerFactory.getLogger(ElasticsearchTest.class.getName());

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

        RestHighLevelClient client = createClient();

        String jsonString = "{\"class\" : \"negative\", \"text\" : \"awful movie\"}";

        IndexRequest indexRequest = new IndexRequest(
          "movie", "rating"
        ).source(jsonString, XContentType.JSON);

        try {
            IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            String id = indexResponse.getId();
            logger.info(id);
        } catch (IOException e) {
            logger.error("error in indexing: ", e);
        }
        try {
            client.close();
        } catch (IOException e) {
            logger.error("error in closing client: ", e);
        }
    }

}














