package com.rabbin.flinkdemo.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    private static final KafkaProducer<String, String> producer = createProducer();

    public static void sendMsg(String topic, String msg) {
        try {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, msg));
            System.out.println(future.get().partition());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("bootstrap.servers", "localhost:9092");
        return new KafkaProducer<>(properties);
    }
}
