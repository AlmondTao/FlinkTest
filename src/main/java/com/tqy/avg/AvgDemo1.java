package com.tqy.avg;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class AvgDemo1 {
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

        dataStreamSource.keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean,Integer, String>() {
                    private ValueState<Tuple2<Integer,Integer>> tupleValues;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        tupleValues = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<Integer, Integer>>("sum-count", Types.TUPLE(Types.INT, Types.INT))
                        );

                    }

                    @Override
                    public void processElement(Integer integer, Context context, Collector<String> collector) throws Exception {

                        if (tupleValues.value() == null){
                            tupleValues.update(Tuple2.of(integer,1));
                        }else{
                            tupleValues.update(Tuple2.of(tupleValues.value().f0+integer,tupleValues.value().f1+1));
                        }


                        collector.collect("总和："+tupleValues.value().f0+",总数："+tupleValues.value().f1+",平均数："+((double)tupleValues.value().f0/tupleValues.value().f1));

                    }


                }).print();

        env.execute();


    }
}
