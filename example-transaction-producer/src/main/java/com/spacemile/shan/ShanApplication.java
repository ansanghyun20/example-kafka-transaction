package com.spacemile.shan;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@SpringBootApplication
public class ShanApplication {

    private static final String TOPIC_NAME = "SHAN_TEST_TRANSACTION"; // 토픽명

    public static void main(String[] args) throws IOException, InterruptedException {

        Random random = new Random();

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092"); // server, kafka host
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("acks", "all");
        prop.put("transactional.id", "SHAN_TEST_TRANSACTION_PRODUCER");
        prop.put("enable.idempotence", "true");

        @SuppressWarnings("resource")
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        producer.initTransactions();
        while (true) {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++) {
                Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(TOPIC_NAME, Integer.toString(i))); // 전송 요청
                try {
                    RecordMetadata metadata = future.get();
                    System.out.println("topic:    " + metadata.topic());
                    System.out.println("partition : " + metadata.partition());
                    System.out.println("offset    : " + metadata.offset());
                    System.out.println("timestamp : " + metadata.timestamp());
                } catch (InterruptedException | ExecutionException e) {
                    producer.abortTransaction();
                }
                Thread.sleep(100); // 0.1초
            }
            producer.commitTransaction();
        }

    }

}
