package com.tqy.richParallelSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class RichParallelSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RichParallelSourceFunction<Integer>() {

            private Boolean running = true;
            private Random random = new Random();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.out.println("SourceFunction open");
            }

            @Override
            public void close() throws Exception {
                super.close();
                System.out.println("SourceFunction close");
            }

            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for(int i = 1 ; i < 10;i++){
                    ctx.collect(i);
                }
            }

            @Override
            public void cancel() {
                System.out.println("SourceFunction cancel");
            }
        }).print().setParallelism(1);

        env.execute();
    }
}
