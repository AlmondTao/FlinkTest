package com.tqy.avg;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

//列表状态变量求平均值
public class AvgDemo3 {
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
                .keyBy(r->true)
                .process(new KeyedProcessFunction<Boolean, Integer, Double>() {

                    private ListState<Integer> integerList;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        integerList = getRuntimeContext().getListState(new ListStateDescriptor("integerList", Types.INT()));
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Double> out) throws Exception {
                        integerList.add(value);
                        Integer count = 0;
                        Integer sum = 0;
                        for(Integer i : integerList.get()){
                            count++;
                            sum = sum+i;
                        }
                        out.collect((double)sum/count);

                    }
                })
                .print().setParallelism(1);

        env.execute();
    }


}
