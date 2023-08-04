package com.tqy.keyProcess;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class KeyProcessDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> stringDataStreamSource = env
                .socketTextStream("192.168.2.100", 9999);

        stringDataStreamSource
                .setParallelism(1)
                .keyBy(r-> true)
                .process(new MyKeyProcess())
                .print();

        env.execute();

    }

    public static class MyKeyProcess extends KeyedProcessFunction<Boolean, String, String>{

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            long l = ctx.timerService().currentProcessingTime();

            out.collect("元素："+value+" 在 "+ new Timestamp(l)+"到达");

            long timerL = l + 10 * 1000L;

            ctx.timerService().registerProcessingTimeTimer(timerL);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器启动:"+new Timestamp(timestamp));
        }
    }
}
