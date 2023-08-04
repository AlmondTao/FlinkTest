package com.tqy.parallelism;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> s1 = env.fromElements("hello world", "hello world").setParallelism(1);

        SingleOutputStreamOperator<WorldCountPOJO2> s2 = s1.flatMap(new FlatMapFunction<String, WorldCountPOJO2>() {
            @Override
            public void flatMap(String s, Collector<WorldCountPOJO2> collector) throws Exception {
                String[] wordArr = s.split(" ");
                for (String word : wordArr) {
                    collector.collect(new WorldCountPOJO2(word, 1));
                }
            }
        }).setParallelism(1);
        KeyedStream<WorldCountPOJO2, String> s3 = s2.keyBy(r -> r.word);

        SingleOutputStreamOperator<WorldCountPOJO2> s4 = s3.reduce(new ReduceFunction<WorldCountPOJO2>() {
            @Override
            public WorldCountPOJO2 reduce(WorldCountPOJO2 t2, WorldCountPOJO2 t1) throws Exception {
                return new WorldCountPOJO2(t1.word, t1.count + t2.count);
            }
        }).setParallelism(1);

        s4.print();

        env.execute();

    }

    public static class WorldCountPOJO2{
        private String word;
        private Integer count;

        public WorldCountPOJO2(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WorldCountPOJO2{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}


