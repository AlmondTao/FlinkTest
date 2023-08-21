package com.tqy.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class TableDemo4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Tuple2<String, String>> stream = env
                .fromElements(
                        Tuple2.of("Mary", "./home"),
                        Tuple2.of("Bob", "./cart"),
                        Tuple2.of("Mary", "./prod?id=1"),
                        Tuple2.of("Liz", "./home")
                );
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);



        Table table = tableEnvironment.fromDataStream(
                stream,
                $("user"),
                $("url"),
                $("user_action_time").proctime()//处理时间
        );

        tableEnvironment.createTemporaryView("click",table);

//        Table table2 = tableEnvironment
//                .sqlQuery("SELECT user,COUNT(url),TUMBLE_START(user_action_time,INTERVAL '1' SECOND) FROM click GROUP BY user, TUMBLE(user_action_time,INTERVAL '1' SECOND)");
//        tableEnvironment.toRetractStream(table2).print();


        Table table2 = tableEnvironment
                .sqlQuery("SELECT user,url,user_action_time FROM click ");
        tableEnvironment.toDataStream(table2).print();


        env.execute();


    }
}
