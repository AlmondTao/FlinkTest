package com.tqy.cep;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

public class CepDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env
                .fromElements(
                        new Event("order-1", "create", 1000L),
                        new Event("order-2", "create", 2000L),
                        new Event("order-1", "pay", 3000L)
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        Pattern<Event, Event> pattern = Pattern
                .<Event>begin("first")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("create");
                    }
                })
                .next("second")
                .where(new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event value) throws Exception {
                        return value.eventType.equals("pay");
                    }
                })
                .within(Time.seconds(5));
        PatternStream<Event> patternStream = CEP.pattern(stream.keyBy(e -> e.orderId), pattern);

        SingleOutputStreamOperator<String> resultStream = patternStream
                .flatSelect(
                        new OutputTag<String>("timeout") {
                        },
                        new PatternFlatTimeoutFunction<Event, String>() {
                            @Override
                            public void timeout(Map<String, List<Event>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
                                List<Event> first = pattern.get("first");
                                out.collect("订单：" + first.get(0).orderId + " 超时了。");
                            }
                        },
                        new PatternFlatSelectFunction<Event, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Event>> pattern, Collector<String> out) throws Exception {
                                List<Event> second = pattern.get("second");
                                out.collect("订单：" + second.get(0).orderId + " 支付成功。");
                            }
                        }
                );

        resultStream.print();
        resultStream.getSideOutput(new OutputTag<String>("timeout"){}).print("timeout:");

        env.execute();


    }
    public static class Event {
        public String orderId;
        public String eventType;
        public Long timestamp;

        public Event() {
        }

        public Event(String orderId, String eventType, Long timestamp) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }
    }
}
