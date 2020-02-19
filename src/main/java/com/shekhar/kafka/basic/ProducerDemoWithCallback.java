package com.shekhar.kafka.basic;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);


    public static void main(String[] args) {

        String hostUrl = "localhost:9092";

        String topic = "first_topic";


        // producer properties
        // check: https://kafka.apache.org/documentation/#producerconfigs
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, hostUrl);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        // create producer
        KafkaProducer producer = new KafkaProducer<String, String>(props);


        // send data - async
        for (int i=0; i<10; ++i) {
            ProducerRecord record = new ProducerRecord<String, String>(topic, "hello world callback - "+i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time when a record is sent successfully or an exception is thrown
                    if (e == null) {
                        logger.info("received new metadata: \n\t" +
                                "Topic: " + recordMetadata.topic() + "\n\t" +
                                "Offset: " + recordMetadata.offset() + "\n\t" +
                                "Partition: " + recordMetadata.partition() + "\n\t" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error in Producing", e);
                    }
                }
            });
        }

        // flush and close producer
        producer.close();

    }

}
