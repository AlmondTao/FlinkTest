package com.tqy.wordcount;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;
import scala.Int;

public class Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //nc -lk 9999
        DataStream<String> dataStream = env.socketTextStream("192.168.2.100", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> s1 = dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] wordArr = s.split(" ");
                        for (String word : wordArr) {
                            collector.collect(Tuple2.<String, Integer>of(word, 1));
                        }

                    }
                });
        KeyedStream<Tuple2<String, Integer>, String> s2 = s1.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> s3 = s2.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t2, Tuple2<String, Integer> t1) throws Exception {
                return Tuple2.of(t1.f0, t1.f1 + t2.f1);
            }
        });

        s3.print();

        env.execute();
    }
}
