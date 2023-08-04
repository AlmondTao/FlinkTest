package com.tqy.aggregateFunction;

import com.tqy.processWindow.ProcessWindowDemo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Calendar;
import java.util.Random;
//聚合函数求每个用户每5秒的平均花销
public class AggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new ClickSource())
                .keyBy(e->e.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAggregateFunction())
                .print();
        env.execute();
    }

    public static class MyAggregateFunction implements AggregateFunction<Event, Tuple2<Long,Integer>,String>{

        @Override
        public Tuple2<Long, Integer> createAccumulator() {
            return Tuple2.of(0L,0);
        }

        @Override
        public Tuple2<Long, Integer> add(Event value, Tuple2<Long, Integer> accumulator) {
            return Tuple2.of(accumulator.f0+value.moneyCost,accumulator.f1+1);
        }

        @Override
        public String getResult(Tuple2<Long, Integer> accumulator) {
            return "平均花销："+accumulator.f0.doubleValue()/accumulator.f1;
        }

        @Override
        public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
            return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
        }
    }

    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"张三","李四","王五","赵六","钱七"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            while (running){
                sourceContext.collect(new Event(
                        userArr[random.nextInt(userArr.length)],
                        new Integer(random.nextInt(1000)).longValue()

                ));
                Thread.sleep( new Integer(random.nextInt(1000)).longValue());
            }

        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    public static class Event{
        private String user;
        private Long moneyCost;

        public Event(String user, Long moneyCost) {
            this.user = user;
            this.moneyCost = moneyCost;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public Long getMoneyCost() {
            return moneyCost;
        }

        public void setMoneyCost(Long moneyCost) {
            this.moneyCost = moneyCost;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", moneyCost=" + moneyCost +
                    '}';
        }
    }
}
