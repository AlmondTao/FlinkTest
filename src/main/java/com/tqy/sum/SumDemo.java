package com.tqy.sum;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SumDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Integer, Integer>> stream = env
                .fromElements(
                        Tuple2.of(1, 3),
                        Tuple2.of(1, 4)
                );

        KeyedStream<Tuple2<Integer, Integer>, Integer> keyStream = stream
                .keyBy(t -> t.f0);
        keyStream
                .sum(1)
                .print("sum:").setParallelism(1);

        keyStream
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> t2, Tuple2<Integer, Integer> t1) throws Exception {
                        return Tuple2.of(t1.f0,Math.max(t1.f1,t2.f1));
                    }
                }).print("max:").setParallelism(1);


        env.execute();

    }

}
