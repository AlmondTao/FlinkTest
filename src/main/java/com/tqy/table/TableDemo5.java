package com.tqy.table;

import com.tqy.waterMarks.WaterMarksDemo4;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

//使用flink sql实现实时热门商品 (滑动窗口)
public class TableDemo5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<UserBehavior> stream = env
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
                                Long.parseLong(splitArr[4]) * 1000L);
                    }
                })
                .filter(ub -> ub.getBehavior().equals("pv"))

                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                                    @Override
                                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                        return element.getTimeStamp();
                                    }
                                })
                );


        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        Table table = tableEnvironment.fromDataStream(
                stream,
                $("userId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timeStamp").rowtime().as("ts")
        );

        tableEnvironment.createTemporaryView("userBehavior",table);


        String innerSql =
                "SELECT " +
                        "itemId," +
                        "COUNT(itemId) as ct," +
                        "HOP_START(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) AS startTime," +
                        "HOP_END(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR) AS endTime " +
                        "FROM " +
                        "userBehavior " +
                        "GROUP BY " +
                        "itemId,HOP(ts,INTERVAL '5' MINUTE,INTERVAL '1' HOUR)";

        String midSql =
                "SELECT " +
                        "itemId," +
                        "ct," +
                        "DATE_FORMAT(startTime,'yyyy-MM-dd HH:mm:ss')," +
                        "DATE_FORMAT(endTime,'yyyy-MM-dd HH:mm:ss')," +
                        "ROW_NUMBER() OVER(PARTITION BY endTime ORDER BY ct DESC) rn " +
                        "FROM (" +
                        innerSql+") ";

        String outerSql =
                "SELECT " +
                        "* " +
                        "FROM (" +
                         midSql+") " +
                        "WHERE " +
                        "rn < 4";

        Table table1 = tableEnvironment
                .sqlQuery(outerSql);

        tableEnvironment.toChangelogStream(table1).print();




        env.execute();


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
