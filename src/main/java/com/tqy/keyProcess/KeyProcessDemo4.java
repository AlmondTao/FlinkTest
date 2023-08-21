package com.tqy.keyProcess;

import com.tqy.dataStreamSource.SourceFunctionDemo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

//用keyProcess模拟滑动窗口+水位线
public class KeyProcessDemo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .setParallelism(1)
                .socketTextStream("192.168.2.100",9999)
                .map(new MapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        Tuple2<String, Long> tuple2 = Tuple2.of(arr[0], (long) (Double.valueOf(arr[1]) * 1000));
                        System.out.println("接收数据："+tuple2.toString());
                        return tuple2;
                    }
                })
                .keyBy(e->e.f0)
                .process(new FakeWindow())
                .print();
        env.execute();

    }

    public static class FakeWindow extends KeyedProcessFunction<String,Tuple2<String, Long>,String>{
        private Long windowSize = 5000L;
        private Long windowSlide = 2000L;
        //延迟时间
        private Long bound = 5000L;
        private Long currentWaterMark = Long.MIN_VALUE + bound - 1;

        private MapState<Long, List<Tuple2<String,Long>>> timerMap;


        @Override
        public void open(Configuration parameters) throws Exception {

            super.open(parameters);

            timerMap = getRuntimeContext().getMapState(new MapStateDescriptor("timerMap", Types.LONG, Types.LIST(Types.TUPLE(Types.STRING,Types.LONG))));

        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            Long valueTime = value.f1;
            if (valueTime < currentWaterMark){
                out.collect("迟到元素："+value);
                return;
            }
            Long valueFinalWindowEndTime = valueTime - valueTime % windowSlide + windowSize - 1;


            while (valueFinalWindowEndTime > valueTime){
                List<Tuple2<String, Long>> tuple2s = timerMap.get(valueFinalWindowEndTime);
                if (tuple2s == null){
                    tuple2s = new ArrayList<Tuple2<String, Long>>();
                    System.out.println("为元素:"+value+" 创建定时器：" + valueFinalWindowEndTime);
                }
                tuple2s.add(value);
                timerMap.put(valueFinalWindowEndTime,tuple2s);
                if (valueFinalWindowEndTime  <  Long.MIN_VALUE  + windowSlide ){
                    break;
                }
                valueFinalWindowEndTime = valueFinalWindowEndTime - windowSlide;
                if (valueFinalWindowEndTime < currentWaterMark ){
                    break;
                }


            }

            currentWaterMark = Math.max(currentWaterMark,valueTime - bound -1);
            ArrayList<Long> needDeletedList = new ArrayList<>();
            for (Long timerMapKey : timerMap.keys()){
                if (timerMapKey <= currentWaterMark){
                    List<Tuple2<String, Long>> tuple2s = timerMap.get(timerMapKey);


                    out.collect("定时器:"+ timerMapKey+" 触发，总共 "+tuple2s.size()+"个元素，分别为："+tuple2s);
                    needDeletedList.add(timerMapKey);

                }
            }
            for (Long  needDeletedKey : needDeletedList){
                timerMap.remove(needDeletedKey);
            }


        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }

        @Override
        public void close() throws Exception {
            super.close();
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
