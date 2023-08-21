package com.tqy.union;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//多条流合并
//所有流的合并必须是同类型事件
public class UnionDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> dataStreamSource1 = env.fromElements(1, 2);

        DataStreamSource<Integer> dataStreamSource2 = env.fromElements(3, 5);
        DataStreamSource<Integer> dataStreamSource3 = env.fromElements(4, 6);


        DataStream<Integer> union = dataStreamSource1.union(dataStreamSource2, dataStreamSource3);

        union.print();

        env.execute();

    }
}
