package com.tqy.checkpoint;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

//WAL机制
public class CheckpointDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionConfig config = env.getConfig();

        TypeSerializer<Event> serializer = TypeInformation.of(Event.class).createSerializer(config);
        env.setParallelism(1);
        env.setStateBackend(new HashMapStateBackend());
        env.enableCheckpointing(5*1000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///FlinkTest/src/main/checkpoint");

        env
                .addSource(new ClickSource(-1))
                .transform("MyWALSink",TypeInformation.of(Event.class),new MyWALSink(new MyCheckpointCommitter(),serializer, UUID.randomUUID().toString()));

        env.execute();




    }


    public static class MyWALSink extends GenericWriteAheadSink<Event>{

        public MyWALSink(CheckpointCommitter committer, TypeSerializer<Event> serializer, String jobID) throws Exception {
            super(committer, serializer, jobID);
        }

        @Override
        protected boolean sendValues(Iterable<Event> values, long checkpointId, long timestamp) throws Exception {
            System.out.println("检查点:"+checkpointId+" 开始发送数据:");
            for (Event event : values){
                System.out.println(event);
            }
            System.out.println("数据发送完毕");
            return true;
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
            if (this.subtaskIdx == subtaskIdx && this.checkpointID == checkpointID){
                this.committed = true;
                System.out.println("checkPointCommitter commitCheckpoint,subtaskIdx:"+subtaskIdx+" checkpointID:"+checkpointID);
            }


        }

        @Override
        public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
            System.out.println("checkPointCommitter isCheckpointCommitted,subtaskIdx:"+subtaskIdx+" checkpointID:"+checkpointID);
            if (this.subtaskIdx == subtaskIdx && this.checkpointID == checkpointID && this.committed == true){
                this.committed = false;
                return true;
            }else {
                return false;
            }


        }
    }
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        private String[] userArr = {"张三","李四","王五","赵六","钱七"};
        private String[] urlArr = {"www.baidu.com","csdn.net","google.com","zhihu.com"};
        private Random random = new Random();
        private int totalCount = 0;

        public ClickSource(int totalCount) {
            if (totalCount == -1){
                this.totalCount = Integer.MAX_VALUE;
            }else{
                this.totalCount = totalCount;
            }

        }

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            int count = 0;
            while (running){

                sourceContext.collect(new Event(
                        userArr[random.nextInt(userArr.length)],
                        urlArr[random.nextInt(urlArr.length)],
                        Calendar.getInstance().getTimeInMillis()
                ));
                count++;

                Thread.sleep(500L);

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
