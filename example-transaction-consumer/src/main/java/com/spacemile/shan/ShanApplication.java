package com.spacemile.shan;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class ShanApplication {

    private static final String TOPIC_NAME = "SHAN_TEST_TRANSACTION";
    public static void main(String[] args) throws IOException, InterruptedException {
        Properties conf = new Properties();

        conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.17.25.40:9092,172.17.25.41:9092,172.17.25.42:9092");
        conf.put(ConsumerConfig.GROUP_ID_CONFIG, "SHAN_TEST_TRANSACTION_CONSUMER");
        conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        conf.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(conf);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records){
                System.out.println();
                System.out.println("offset : {} key : {} value : {}" + record.offset() + record.key() + record.value());
                System.out.println();
            }
        }


    }
}
