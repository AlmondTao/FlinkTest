package com.tqy.test;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.scala.typeutils.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

//连续一秒上升报警
public class Demo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .addSource(new SourceFunction<Integer>() {
                    private Random random = new Random();
                    private Boolean running = true;
                    private Long sleepTime = 300L;

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {
                        while (running){
                            ctx.collect(random.nextInt(100));
                            Thread.sleep(sleepTime);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(i -> true)
                .process(new KeyedProcessFunction<Boolean, Integer, String>() {

                    private ValueState<Integer> integerValue;
                    private ValueState<Long> timerStamp;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        integerValue = getRuntimeContext().getState(new ValueStateDescriptor("integerValue", Types.INT()));
                        timerStamp = getRuntimeContext().getState(new ValueStateDescriptor("timerStamp", Types.LONG()));
                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        Integer preValue = integerValue.value();
                        if (integerValue.value() == null){
                            preValue = value;
                        }

                        if (preValue < value && timerStamp.value() == null){
                            long currentTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentTime+1000L);
                            timerStamp.update(currentTime+1000L);

                        }

                        if (preValue > value && timerStamp.value() != null){
                            ctx.timerService().deleteEventTimeTimer(timerStamp.value());

                        }

                        integerValue.update(value);
                        out.collect("数字："+value);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("连续1秒上升报警，报警时间："+new Timestamp(timerStamp.value()));
                        timerStamp.clear();
                    }
                })
                .print().setParallelism(1);

        env.execute();

    }
}
