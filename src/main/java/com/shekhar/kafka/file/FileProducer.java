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
        logger.info("setup");

        ReadResourceFile readResourceFile = new ReadResourceFile();
        String fileName = "movie_reviews.txt";
        List<String> lines = readResourceFile.read(fileName);


        // create producer
        String topic = "movie_topic";
//        KafkaProducer<String, String> producer = createProducer();


        // send data to producer
        for (String msg : lines) {

            JSONObject json = new JSONObject(msg);
            String key = "no-class";
            if (json.has("class")) {
                key = json.getString("class");
            }
            System.out.println(topic + " >> " + key + " >> " + msg);

//            logger.info(msg);
//            producer.send(new ProducerRecord<>(topic, null, msg), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e != null) {
//                        logger.error("something bad happended.", e);
//                    }
//                }
//            });
        }

        logger.info("end");
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
