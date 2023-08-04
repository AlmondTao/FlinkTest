package com.tqy.avg;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AvgDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> dataStreamSource = env.addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (running) {
                    sourceContext.collect(random.nextInt(1000));
                    Thread.sleep(1000);
                }

            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        dataStreamSource
                .map(i -> Tuple2.of(i,1))
                .returns(Types.TUPLE(Types.INT,Types.INT))
                .keyBy(t-> true)
                .reduce((t1,t2)->Tuple2.of(t1.f0+t2.f0,t1.f1+t2.f1))
                .map(t->"总和："+t.f0+",总数："+t.f1+",平均数："+(double)t.f0/t.f1)
                .print();

        env.execute();


    }
}
