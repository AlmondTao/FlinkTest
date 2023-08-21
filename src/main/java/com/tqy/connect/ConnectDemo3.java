package com.tqy.connect;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Event> orderStream = env.fromElements(
                Event.of("order-1", "order", 1000L),
                Event.of("order-2", "order", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        SingleOutputStreamOperator<Event> weixinStream = env.fromElements(
                Event.of("order-2", "weixin", 3000L),
                Event.of("order-3", "weixin", 5000L),
                Event.of("order-1", "weixin", 30000L)

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event element, long recordTimestamp) {
                return element.timestamp;
            }
        }));

        
        orderStream
                .keyBy(e -> e.orderId)
                .connect(weixinStream.keyBy(e->e.orderId))
                .process(new CoProcessFunction<Event, Event, String>() {
                    private ValueState<Event> orderState;
                    private ValueState<Event> weixinState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        orderState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("orderState", Types.POJO(Event.class)));
                        weixinState = getRuntimeContext().getState(new ValueStateDescriptor<Event>("weixinState", Types.POJO(Event.class)));

                    }

                    @Override
                    public void processElement1(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (weixinState.value() == null){
                            orderState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.timestamp+5000L);
                        }else{
                            out.collect("订单："+value.orderId+"对账完成!weixinState先到。");
                            weixinState.clear();
                        }


                    }

                    @Override
                    public void processElement2(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (orderState.value() == null){
                            weixinState.update(value);
                            ctx.timerService().registerEventTimeTimer(value.timestamp+5000L);
                        }else{
                            out.collect("订单："+value.orderId+"对账完成!orderState先到。");
                            orderState.clear();
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if (orderState.value() != null) {
                            out.collect("订单ID" + orderState.value().orderId + "对账失败，weixin事件5s内未到达");
                            orderState.clear();
                        }
                        if (weixinState.value() != null) {
                            out.collect("订单ID" + weixinState.value().orderId + "对账失败，order事件5s内未到达");
                            weixinState.clear();
                        }
                    }
                })
                .print();

        System.out.println(env.getExecutionPlan());

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

        public static Event of(String orderId, String eventType, Long timestamp) {
            return new Event(orderId, eventType, timestamp);
        }

        @Override
        public String toString() {
            return "Event{" +
                    "orderId='" + orderId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
