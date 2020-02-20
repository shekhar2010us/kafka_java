package com.shekhar.kafka.stream;

import com.fasterxml.jackson.core.JsonParser;
import com.shekhar.kafka.basic.ReadResourceFile;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class StreamFilterMovieRatings {

    final static String topic = "movie_topic";
    final static String newTopic = "new_movie_topic";
    final static String negFileName = "negative.words";
    final static String posFileName = "positive.words";
    static List<String> posWords = new ArrayList<String>();
    static List<String> negWords = new ArrayList<String>();


    public static void main(String[] args) {

        // properties
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // load necessary process files
        posWords = readFileInList(posFileName);
        negWords = readFileInList(negFileName);

        // create stream topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(topic);
        KStream<String, String> mappedStream = inputTopic.mapValues(
                (k, jsonRating) -> processRating(jsonRating)
        );

//        KStream<String, String> filteredStream = inputTopic.filter(
//                (k, jsonRating) -> processor(jsonRating) > 10
//        );

        mappedStream.to(newTopic);
//        filteredStream.to(newTopic);

        // build stream topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                props
        );

        // start stream application
        kafkaStreams.start();


    }

    private static List<String> readFileInList(String fileName) {
        ReadResourceFile readResourceFile = new ReadResourceFile();
        List<String> lines = readResourceFile.read(fileName);
        return lines;
    }

    private static Integer processor(String ratingJson) {
        return -10;
    }

    private static String processRating(String ratingJson) {

        JSONObject jsonObj = new JSONObject(ratingJson);
        String prediction = "neutral";
        if (jsonObj.has("text") && jsonObj.has("class")) {
            int posCount = 0;
            int negCount = 0;

            String text = jsonObj.getString("text").trim().toLowerCase();
            String actualSent = jsonObj.getString("class").trim().toLowerCase();

            String[] tokens = text.split(" ");
            for (String token:tokens) {
                if (posWords.contains(token))
                    posCount++;
                if (negWords.contains(token))
                    negCount++;
            }
            if (posCount > negCount)
                prediction = "positive";
            else if (negCount > posCount)
                prediction = "negative";
            else
                prediction = "neutral";

            if (actualSent.equalsIgnoreCase(prediction)) {
                return "Predicted Correctly";
            } else {
                return "I am not intelligent enough to predict this correctly";
            }


        } else {
            return "record doesn't have neccessary field";
        }
    }

}
