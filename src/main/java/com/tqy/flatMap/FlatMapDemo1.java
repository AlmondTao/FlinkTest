package com.tqy.flatMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.fromElements("red", "yellow", "blue");

        stream.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                if (s.equals("red")) collector.collect(s);
                else if (s.equals("yellow")) {collector.collect(s); collector.collect(s);}
                else if (s.equals("blue")) {collector.collect(s); collector.collect(s); collector.collect(s);}
            }
        }).print().setParallelism(1);

        env.execute();

    }
}
