package com.rabbin.flinkdemo.apps.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafKaDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder().setTopics("test")
                .setGroupId("test")
                .setBootstrapServers("localhost:9092")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka source").map(new MapFunction<String, String>() {
            @Override
            public String map(String msg) {
                return "xxx" + msg;
            }
        }).print();

        env.execute();
    }
}
