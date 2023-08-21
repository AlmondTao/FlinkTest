package com.tqy.waterMarks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collection;

//trigger
public class WaterMarksDemo9 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env
                .socketTextStream("192.168.2.100",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        Tuple2<String, Long> tuple2 = Tuple2.of(arr[0], (long) (Double.valueOf(arr[1]) * 1000));
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                .trigger(new Trigger<Tuple2<String, Long>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("调用onElement");
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("调用onProcessingTime");
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        System.out.println("调用onEventTime");
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        long windowStart = context.window().getStart();
                        long windowEnd   = context.window().getEnd();
                        long count       = elements.spliterator().getExactSizeIfKnown(); // 迭代器里面共多少条元素
                        out.collect("用户：" + key + " 在窗口" +
                                "" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd) + "" +
                                "中的pv次数是：" + count);
                    }
                })
                .print();

        env.execute();


    }

    public static class MyWindowAssigner extends WindowAssigner<Tuple2<String,Long>, GlobalWindow>{
        @Override
        public Collection<GlobalWindow> assignWindows(Tuple2<String, Long> element, long timestamp, WindowAssignerContext context) {

            return null;
        }

        @Override
        public Trigger<Tuple2<String, Long>, GlobalWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return null;
        }

        @Override
        public TypeSerializer<GlobalWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return null;
        }

        @Override
        public boolean isEventTime() {
            return false;
        }
    }
}
