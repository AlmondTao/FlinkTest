package com.tqy.intervalJoin;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

//基于间隔join
public class IntervalJoinDemo1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Event> orderStream = env.fromElements(
                Event.of("user-1", "order", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1*60*1000L)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        SingleOutputStreamOperator<Event> pvStream = env.fromElements(
                Event.of("user-1", "pv", 5 * 60 * 1000L),

                Event.of("user-1", "pv", 12 * 60 * 1000L),
                Event.of("user-1", "pv", 25 * 60 * 1000L),
                Event.of("user-1", "pv", 26 * 60 * 1000L),
                Event.of("user-1", "pv", 10 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                })
        );

        orderStream
                .keyBy(e -> e.userId)
                .intervalJoin(pvStream.keyBy(e->e.userId))
                .between(Time.minutes(-10),Time.minutes(5))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(left +" => " +right);
                    }
                })
                .print("order join pv:");

        pvStream
                .keyBy(e -> e.userId)
                .intervalJoin(orderStream.keyBy(e->e.userId))
                .between(Time.minutes(-5),Time.minutes(10))
                .process(new ProcessJoinFunction<Event, Event, String>() {
                    @Override
                    public void processElement(Event left, Event right, Context ctx, Collector<String> out) throws Exception {
                        out.collect(right+" => " +left );
                    }
                })
                .print("pv join order:");

        env.execute();





    }

    public static class Event {
        public String userId;
        public String eventType;
        public Long timestamp;

        public Event() {
        }

        public Event(String userId, String eventType, Long timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public static Event of(String userId, String eventType, Long timestamp) {
            return new Event(userId, eventType, timestamp);
        }

        @Override
        public String toString() {
            return "Event{" +
                    "userId='" + userId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", timestamp=" + new Timestamp(timestamp) +
                    '}';
        }
    }
}
