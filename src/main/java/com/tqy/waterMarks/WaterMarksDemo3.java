package com.tqy.waterMarks;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

//滚动窗口统计pv
public class WaterMarksDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .socketTextStream("192.168.2.100",9999)

                .map(new MapFunction<String, org.apache.flink.api.java.tuple.Tuple2<String,Long>>() {
                    @Override
                    public org.apache.flink.api.java.tuple.Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        org.apache.flink.api.java.tuple.Tuple2<String, Long> tuple2 = org.apache.flink.api.java.tuple.Tuple2.of(arr[0], (long) (Double.valueOf(arr[1]) * 1000));
                        System.out.println("接收数据："+tuple2.toString());
                        return tuple2;
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String,Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String,Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
                )
                .keyBy(t->t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long watermark = context.currentWatermark();
                        long startTime = context.window().getStart();
                        long endTime = context.window().getEnd();
                        long count       = elements.spliterator().getExactSizeIfKnown(); // 迭代器里面共多少条元素
                        out.collect("水位线："+new Timestamp(watermark)+" 用户：" + key + " 在窗口" +
                                "" + new Timestamp(startTime) + "~" + new Timestamp(endTime) + "" +
                                "中的pv次数是：" + count);
                    }
                })
                .print();

        env.execute();
    }
}
