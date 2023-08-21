package com.tqy.join;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class JoinDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> tuple2Stream = env.fromElements(
                Tuple2.of("a", 1L),
                Tuple2.of("a", 70L),
                Tuple2.of("a", 70L),
                Tuple2.of("a", 70L),
                Tuple2.of("a", 70L),
                Tuple2.of("a", 70L),
                Tuple2.of("b", 80L),
                Tuple2.of("b", 3L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1*1000L;
                    }
                })
        );

        SingleOutputStreamOperator<Tuple3<String, Long, String>> tuple3Stream = env.fromElements(
                Tuple3.of("a", 1L, "aa"),
                Tuple3.of("a", 60L, "aa"),
                Tuple3.of("a", 60L, "aa"),
                Tuple3.of("a", 60L, "aa"),
                Tuple3.of("a", 60L, "aa"),
                Tuple3.of("a", 60L, "aa"),
                Tuple3.of("a", 80L, "aa"),
                Tuple3.of("b", 4L, "bb"),
                Tuple3.of("b", 3L, "bb")
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, String>>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                        return element.f1 * 1000L;
                    }
                })
        );

        tuple2Stream
                .join(tuple3Stream)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new Trigger<CoGroupedStreams.TaggedUnion<Tuple2<String, Long>, Tuple3<String, Long, String>>, TimeWindow>() {

                    @Override
                    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
                        super.onMerge(window, ctx);
                    }

                    @Override
                    public TriggerResult onElement(CoGroupedStreams.TaggedUnion<Tuple2<String, Long>, Tuple3<String, Long, String>> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return null;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .apply(new JoinFunction<Tuple2<String, Long>, Tuple3<String, Long, String>, String>() {
                    @Override
                    public String join(Tuple2<String, Long> first, Tuple3<String, Long, String> second) throws Exception {
                        return first +" => " + second;
                    }
                }).print();

        env.execute();


    }
}
