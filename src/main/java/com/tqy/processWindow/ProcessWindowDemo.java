package com.tqy.processWindow;

import com.tqy.state.MapStateDemo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Random;

//每个用户每5s窗口的pv
public class ProcessWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new ClickSource())
                .keyBy(r->r.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new MyWindow())
                .print().setParallelism(1);
        env.execute();
    }

    public static class MyWindow extends ProcessWindowFunction<Event,String,String, TimeWindow> {

        @Override
        public void process(String key, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            long startTime = context.window().getStart();
            long endTime = context.window().getEnd();

            out.collect("用户："+key+"在窗口 "+new Timestamp(startTime)+" 到 "+new Timestamp(endTime)
            +" pv次数为："+elements.spliterator().getExactSizeIfKnown());
        }


    }

    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"张三","李四","王五","赵六","钱七"};
        private String[] urlArr = {"www.baidu.com","csdn.net","google.com","zhihu.com"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            while (running){
                sourceContext.collect(new Event(
                        userArr[random.nextInt(userArr.length)],
                        urlArr[random.nextInt(urlArr.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                Thread.sleep(1000L);
            }

        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    public static class Event{
        private String user;
        private String url;
        private Long timestamp;

        public Event(String user, String url, Long timestamp) {
            this.user = user;
            this.url = url;
            this.timestamp = timestamp;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}
