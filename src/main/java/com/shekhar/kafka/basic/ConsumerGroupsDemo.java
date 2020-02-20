package com.shekhar.kafka.basic;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerGroupsDemo {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerGroupsDemo.class.getName());

        String hostUrl = "127.0.0.1:9092";
        String topic = "first_topic";
        String groupId = "my-app-2";

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

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record: records) {
                record.key();
                record.value();
                record.offset();
                record.partition();

                logger.info("received new message: \n\t" +
                        "Key: " + record.key() + "\n\t" +
                        "Value: " + record.value() + "\n\t" +
                        "Partition: " + record.partition() + "\n\t" +
                        "Offset: " + record.offset() + "\n");

            }
        }

    }

}
