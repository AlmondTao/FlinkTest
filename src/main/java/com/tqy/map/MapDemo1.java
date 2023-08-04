package com.tqy.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MapDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Integer> s1 = env.addSource(new SourceFunction<Integer>() {
            private boolean running = true;
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (running) {
                    sourceContext.collect(random.nextInt(1000));

                    Thread.sleep(1000L);
                }

            }

            @Override
            public void cancel() {
                running = false;
            }
        });
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> s2 = s1.map(r -> Tuple2.of(r, r));
        //类型擦除，需要标准返回值类型，否则报错
        s2.returns(Types.TUPLE(Types.INT,Types.INT));
        s2.print("map1");

        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(random.nextInt(1000));

                            Thread.sleep(1000L);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MapFunction<Integer, Tuple2<Integer,Integer>>() {

                    @Override
                    public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
                        return Tuple2.of(integer,integer);
                    }
                }).print("map2");
        env
                .addSource(new SourceFunction<Integer>() {
                    private boolean running = true;
                    private Random random = new Random();

                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        while (running) {
                            sourceContext.collect(random.nextInt(1000));

                            Thread.sleep(1000L);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .map(new MyMap())
                .print("map3");


        env.execute();

    }

    public static class MyMap implements MapFunction<Integer,Tuple2<Integer,Integer>>{

        @Override
        public Tuple2<Integer,Integer> map(Integer integer) throws Exception {
            return Tuple2.of(integer,integer);
        }
    }
}
