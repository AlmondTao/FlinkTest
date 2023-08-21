package com.tqy.checkpoint;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;

import java.util.Calendar;
import java.util.Random;

//WAL机制
public class CheckpointDemo2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(10*1000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///FlinkTest/src/main/checkpoint");

        env
                .addSource(new ClickSource());

    }


    public static class MyWALSink extends GenericWriteAheadSink<Event>{

        public MyWALSink(CheckpointCommitter committer, TypeSerializer<Event> serializer, String jobID) throws Exception {
            super(committer, serializer, jobID);
        }

        @Override
        protected boolean sendValues(Iterable<Event> values, long checkpointId, long timestamp) throws Exception {
            return false;
        }
    }

    public static class MyCheckpointCommitter extends CheckpointCommitter{
        private int subtaskIdx;
        private long checkpointID;
        private boolean committed = false;
        @Override
        public void open() throws Exception {
            System.out.println("checkPointCommitter open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("checkPointCommitter close");
        }

        @Override
        public void createResource() throws Exception {
            System.out.println("checkPointCommitter createResource");
        }

        @Override
        public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
            this.subtaskIdx = subtaskIdx;
            this.checkpointID = checkpointID;
            this.committed = true;
            System.out.println("checkPointCommitter commitCheckpoint");
        }

        @Override
        public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
            return this.committed;
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
