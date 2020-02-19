package com.shekhar.kafka.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {
        System.out.println("Hello");
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
        ProducerRecord record = new ProducerRecord<String, String>(topic, "hello world basic");
        producer.send(record);

        // flush and close producer
        producer.close();

    }

}
