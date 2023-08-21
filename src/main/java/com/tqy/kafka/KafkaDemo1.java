package com.tqy.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();
        properties.put("bootstrap.servers","192.168.2.100:9092,192.168.2.110:9092,192.168.2.120:9092");

        env
                .readTextFile("D:\\FlinkTest\\src\\main\\resources\\UserBehavior.csv")
                .addSink(new FlinkKafkaProducer<String>(
                        "user-behavior-1",
                        new SimpleStringSchema(),
                        properties
                ));
        env.execute();

    }
}
