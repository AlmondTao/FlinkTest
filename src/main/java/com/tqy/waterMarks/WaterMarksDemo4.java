package com.tqy.waterMarks;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

//每个窗口中最热门的商品是什么
public class WaterMarksDemo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .readTextFile("D:\\FlinkTest\\src\\main\\resources\\UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
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
                })
                .filter(ub->ub.getBehavior().equals("pv"))

                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimeStamp();
                            }
                        })
                )
                .keyBy(u -> u.getItemId())
                .window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)))
                .aggregate(new CountAggregate(),new MyWindowFunction())
                .keyBy(e->e.windowEndTime)
                .process(new TopN(3))
                .print();
        env.execute();


    }

    public static class TopN extends KeyedProcessFunction<Long,WindowEvent,String>{

        private Integer n;
        private ListState<WindowEvent> windowEventList;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            windowEventList = getRuntimeContext().getListState(new ListStateDescriptor<WindowEvent>("windowEventList", Types.POJO(WindowEvent.class)));

        }

        @Override
        public void processElement(WindowEvent value, Context ctx, Collector<String> out) throws Exception {
            windowEventList.add(value);
            ctx.timerService().registerEventTimeTimer(value.windowEndTime+1L);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            ArrayList<WindowEvent> windowEvents = new ArrayList<>();
            for (WindowEvent event:windowEventList.get()){
                windowEvents.add(event);
            }
            windowEvents.sort(new Comparator<WindowEvent>() {
                @Override
                public int compare(WindowEvent o1, WindowEvent o2) {
                    return o2.count - o1.count;
                }
            });
            StringBuilder valueStr = new StringBuilder();
            String itemId = windowEvents.get(0).itemId;
            valueStr.append("#########################\n");
            valueStr.append("窗口结束时间:"+new Timestamp(timestamp-1L))
                    .append("\n");
            for (int i = 1;i < n+1; i++){
                valueStr.append("第"+i+"个商品id为:"+windowEvents.get(i-1).itemId+" ，浏览次数为："+windowEvents.get(i-1).count+"\n");

            }

            out.collect(valueStr.toString());




        }
    }

    public static class MyWindowFunction extends ProcessWindowFunction<Integer, WindowEvent, String, TimeWindow> {


        @Override
        public void process(String key, Context context, Iterable<Integer> elements, Collector<WindowEvent> out) throws Exception {
            out.collect(new WindowEvent(key,elements.iterator().next(),context.window().getStart(),context.window().getEnd()));
        }
    }

    public static class CountAggregate implements AggregateFunction<UserBehavior,Integer,Integer>{

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(UserBehavior value, Integer accumulator) {
            return accumulator+1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

    public static class WindowEvent{
        private String itemId;
        private Integer count;
        private Long windowStartTime;
        private Long windowEndTime;

        public WindowEvent() {
        }

        public WindowEvent(String itemId, Integer count, Long windowStartTime, Long windowEndTime) {
            this.itemId = itemId;
            this.count = count;
            this.windowStartTime = windowStartTime;
            this.windowEndTime = windowEndTime;
        }

        public String getItemId() {
            return itemId;
        }

        public void setItemId(String itemId) {
            this.itemId = itemId;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public Long getWindowStartTime() {
            return windowStartTime;
        }

        public void setWindowStartTime(Long windowStartTime) {
            this.windowStartTime = windowStartTime;
        }

        public Long getWindowEndTime() {
            return windowEndTime;
        }

        public void setWindowEndTime(Long windowEndTime) {
            this.windowEndTime = windowEndTime;
        }

        @Override
        public String toString() {
            return "WindowEvent{" +
                    "itemId='" + itemId + '\'' +
                    ", count=" + count +
                    ", windowStartTime=" + windowStartTime +
                    ", windowEndTime=" + windowEndTime +
                    '}';
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
