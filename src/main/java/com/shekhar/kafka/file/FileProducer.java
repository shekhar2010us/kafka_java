package com.shekhar.kafka.file;

import com.shekhar.kafka.basic.ReadResourceFile;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class FileProducer {

    static final Logger logger = LoggerFactory.getLogger(FileProducer.class.getName());

    public FileProducer() {}

    public static void main(String[] args) {
        new FileProducer().run();
    }

    public void run() {
        logger.info("setup the application");

        ReadResourceFile readResourceFile = new ReadResourceFile();
        String fileName = "movie_reviews.txt";
        List<String> lines = readResourceFile.read(fileName);


        // create producer
        String topic = "movie_topic";
        KafkaProducer<String, String> producer = createProducer();


        // send data to producer
        int N=0;
        for (String msg : lines) {
            N++;

            JSONObject json = new JSONObject(msg);
            String key = "";
            String value = "";
            if (json.has("class")) {
                key = json.getString("class") + "_" + N;
            }
            value = msg;
            logger.info(key, value);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Error in sleep: " , e);
            }

            producer.send(new ProducerRecord<>(topic, key, value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("something wrong happended.", e);
                    }
                }
            });
        }

        logger.info("end of the application");
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
