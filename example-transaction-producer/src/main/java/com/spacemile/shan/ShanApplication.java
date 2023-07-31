package com.spacemile.shan;

import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@SpringBootApplication
public class ShanApplication {

	private static final String TOPIC_NAME = "SHAN_TEST_TRANSACTION"; //토픽명
	
	public static void main(String[] args) throws IOException, InterruptedException {
	
	    Random random = new Random();
            
            Properties prop = new Properties();
            prop.put("bootstrap.servers", "172.17.25.40:9092,172.17.25.41:9092,172.17.25.42:9092"); // server, kafka host
            prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");   
            prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); 
            prop.put("acks", "all");   
			prop.put("transactional.id", "SHAN_TEST_TRANSACTION_PRODUCER");
			prop.put("enable.idempotence", "true");

            String message = null;

            // producer 생성
            @SuppressWarnings("resource")
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
            producer.initTransactions();
            // message 전달
            while(true) {
                
                producer.beginTransaction();
                for(int i=0; i<100; i++){
                    message = Integer.toString(random.nextInt(100)); // 1~100 중 랜덤숫자
                    producer.send(new ProducerRecord<String, String>(TOPIC_NAME, message));
                    Thread.sleep(100); // 0.1초
                }
                producer.commitTransaction();
            }

        }

}
