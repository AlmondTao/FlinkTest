package com.tqy.keyProcess;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class KeyProcessDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env
                .setParallelism(1)
                .socketTextStream("192.168.2.100",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] split = value.split(" ");
                        Tuple2<String, Long> tuple = Tuple2.of(split[0], Long.parseLong(split[1]) * 1000L);
                        System.out.println("接收到数据："+tuple.toString());
                        return tuple;
                    }
                })

                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        }))


                .keyBy(t->t.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(3)))
                .aggregate(new MyAgg(),new MyWindowProcess())
                .keyBy(t->t.f3)
                .process(new MyKeyProcess())
                .print();

        env.execute();

    }

    public static class MyWindowProcess extends ProcessWindowFunction<Integer, Tuple4<String,Integer,Long,Long>,String, TimeWindow>{

        @Override
        public void process(String s, Context context, Iterable<Integer> elements, Collector<Tuple4<String, Integer, Long, Long>> out) throws Exception {
            Integer next = elements.iterator().next();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            //窗口时间为
            System.out.println("窗口 开始时间："+new Timestamp(start)+",结束时间："+new Timestamp(end)+"。关闭");
            out.collect(Tuple4.of(s,next,start,end));
        }
    }

    public static class MyAgg implements AggregateFunction<Tuple2<String,Long>, Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Tuple2<String, Long> value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }

    public static class MyKeyProcess extends KeyedProcessFunction<Long, Tuple4<String,Integer,Long,Long>, String>{


        @Override
        public void processElement(Tuple4<String, Integer, Long, Long> value, Context ctx, Collector<String> out) throws Exception {
            long registerTimer = value.f3 + 1000L;
            ctx.timerService().registerEventTimeTimer(registerTimer);
            out.collect("注册了一个定时器："+new Timestamp(registerTimer)+",数据为："+value.toString());

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            out.collect("定时器："+new Timestamp(timestamp)+" 触发");
        }
    }
}
