package com.rabbin.flinkdemo.apps.kafka;


import com.rabbin.flinkdemo.utils.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Emulator {

    public static void main(String[] args) {
        String topic = "test";
        String msg = "hello";
        Producer.sendMsg(topic, msg);
    }
}
