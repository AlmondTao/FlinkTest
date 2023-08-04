package com.tqy.state;

import com.tqy.dataStreamSource.SourceFunctionDemo;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;
//用mapstate记录用户pv
public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(new ClickSource())
                .keyBy(r->true)
                .process(new KeyedProcessFunction<Boolean,Event, String>() {

                    private MapState<String, Map<String,Integer>> userMap;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        userMap = getRuntimeContext().getMapState(new MapStateDescriptor<String, Map<String, Integer>>("userMap", Types.STRING, Types.MAP(Types.STRING, Types.INT)));
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Map<String, Integer> userUrlMap = userMap.get(value.user);
                        if (userUrlMap == null ){
                            userUrlMap = new HashMap<>();
                            userUrlMap.put(value.getUrl(),1);
                        }else{
                            Integer integer = userUrlMap.get(value.getUrl());
                            if (integer == null){
                                userUrlMap.put(value.getUrl(),1);
                            }else{
                                userUrlMap.put(value.getUrl(),integer+1);
                            }
                        }
                        userMap.put(value.user,userUrlMap);
                        StringBuilder outStr = new StringBuilder();
                        for (String userMapKey:userMap.keys()){
                            outStr.append(userMapKey+":\n");
                            Map<String, Integer> userUrl = userMap.get(userMapKey);
                            for (String urlStr: userUrl.keySet()){
                                outStr.append("\t网站:"+urlStr+",浏览次数:"+userUrl.get(urlStr)+"\n");
                            }


                        }
                        outStr.append("########################\n");
                        out.collect(outStr.toString());

                    }
                })
                .print().setParallelism(1);

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
