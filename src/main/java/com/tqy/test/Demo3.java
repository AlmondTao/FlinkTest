package com.tqy.test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnel;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;


//(用布隆过滤器)求一个小时内的独立访客的uv
//统计的时候并行度必须为1
public class Demo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env
                .readTextFile("D:\\FlinkTest\\src\\main\\resources\\UserBehavior.csv")
                .map(
                        new MapFunction<String, UserBehavior>() {
                            @Override
                            public UserBehavior map(String value) throws Exception {
                                String[] splitArr = value.split(",");
                                return new UserBehavior(
                                        splitArr[0],
                                        splitArr[1],
                                        splitArr[2],
                                        splitArr[3],
                                        Long.parseLong(splitArr[4])*1000L);

                            }
                        }
                )
                .filter(u -> u.behavior.equals("pv"))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.timeStamp;
                            }
                        })
                )
                .keyBy(u -> true)
                .window(TumblingEventTimeWindows.of(Time.hours(1L)))
                .aggregate(new CountAgg(),new MyWindowFunction())
                .print();

        env.execute();




    }

    public static class MyWindowFunction extends ProcessWindowFunction<Long,String,Boolean, TimeWindow>{

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();

            out.collect("在窗口："+ new Timestamp(start) +" 到 "+ new Timestamp(end) + " 之间的 uv为："+elements.iterator().next());

        }
    }


    public static class CountAgg implements AggregateFunction<UserBehavior, Tuple2<Long,BloomFilter<String>>, Long>{
        @Override
        public Tuple2<Long,BloomFilter<String>> createAccumulator() {

            /**
             * Funnel，这是Guava中定义的一个接口，它和PrimitiveSink配套使用，
             * 主要是把任意类型的数据转化成Java基本数据类型（primitive value，如char，byte，int……），
             * expectedInsertions 期望插入数据数，int或long
             * 默认用java.nio.ByteBuffer实现，最终均转化为byte数组
             * fpp期望误判率，比如1E-7（千万分之一）
             */

            return Tuple2.of(0L,BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8),100000,0.1));
        }

        @Override
        public Tuple2<Long,BloomFilter<String>> add(UserBehavior value, Tuple2<Long,BloomFilter<String>> accumulator) {
            if (!accumulator.f1.mightContain(value.getUserId())){
                accumulator.f0+=1L;
                accumulator.f1.put(value.getUserId());
            }


            return accumulator;
        }

        @Override
        public Long getResult(Tuple2<Long,BloomFilter<String>> accumulator) {
            return accumulator.f0;
        }

        @Override
        public Tuple2<Long,BloomFilter<String>> merge(Tuple2<Long,BloomFilter<String>> a, Tuple2<Long,BloomFilter<String>> b) {


            return a;
        }
    }

    public static class UserBehavior{
        private String userId;
        private String itemId;
        private String categoryId;
        private String behavior;
        private Long timeStamp;

        public UserBehavior() {
        }

        public UserBehavior(String userId, String itemId, String categoryId, String behavior, Long timeStamp) {
            this.userId = userId;
            this.itemId = itemId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timeStamp = timeStamp;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getItemId() {
            return itemId;
        }

        public void setItemId(String itemId) {
            this.itemId = itemId;
        }

        public String getCategoryId() {
            return categoryId;
        }

        public void setCategoryId(String categoryId) {
            this.categoryId = categoryId;
        }

        public String getBehavior() {
            return behavior;
        }

        public void setBehavior(String behavior) {
            this.behavior = behavior;
        }

        public Long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(Long timeStamp) {
            this.timeStamp = timeStamp;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId='" + userId + '\'' +
                    ", itemId='" + itemId + '\'' +
                    ", categoryId='" + categoryId + '\'' +
                    ", behavior='" + behavior + '\'' +
                    ", timeStamp=" + timeStamp +
                    '}';
        }
    }
}
