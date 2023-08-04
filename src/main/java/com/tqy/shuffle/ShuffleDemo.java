package com.tqy.shuffle;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ShuffleDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .shuffle()
                .print("shuffle:").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .rebalance()
                .print("rebalance:").setParallelism(2);

        env
                .fromElements(1,2,3,4).setParallelism(1)
                .broadcast()
                .print("broadcast:").setParallelism(2);




        env.execute();
    }
}
