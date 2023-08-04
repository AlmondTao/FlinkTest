package com.tqy.dataStreamSource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SourceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> s1 = env.addSource(new ClickSource());

        s1.print();

        env.execute();


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
