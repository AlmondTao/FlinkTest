package com.tqy.allowedLateness;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class AllowedLatenessDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        OutputTag<Tuple2<String,Long>> output = new OutputTag<Tuple2<String,Long>>("output"){};
        SingleOutputStreamOperator<String> result = env
                .setParallelism(1)
                .socketTextStream("192.168.2.100", 9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                })
                )
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(output)
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        ValueState<Boolean> first = context.windowState().getState(new ValueStateDescriptor<Boolean>("first", Types.BOOLEAN));
                        StringBuilder elementStr = new StringBuilder();
                        for (Tuple2<String, Long> tuple :elements){
                            elementStr.append(tuple+" ");
                        }
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        if (first.value() == null) {
                            out.collect("窗口第一次触发计算了！水位线是："
                                    + context.currentWatermark() + " 窗口:"+start+" 到 " + end + " 中共有 "
                                    + elements.spliterator().getExactSizeIfKnown()
                                    +" 分别是："+elementStr);
                            first.update(true); // 第一次触发process执行以后，更新为true
                        } else {
                            out.collect("迟到数据到了，"+ context.currentWatermark() + " 窗口:"+start+" 到 " + end +" 更新以后的计算结果是：" + elements.spliterator().getExactSizeIfKnown()+" 分别是："+elementStr);
                        }
                    }
                });
        result.print();
        result.getSideOutput(output).print();

        env.execute();


    }
}
