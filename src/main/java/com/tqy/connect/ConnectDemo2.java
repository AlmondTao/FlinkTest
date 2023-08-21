package com.tqy.connect;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

//实现两个流关联查询输出
//类似：SELECT * FROM A INNER JOIN B WHERE A.id=B.id;
public class ConnectDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Tuple2<String, Integer>> stream1 = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3),
                Tuple2.of("a", 11)
        );


        DataStreamSource<Tuple2<String, String>> stream2 = env.fromElements(
                Tuple2.of("a", "aa"),
                Tuple2.of("b", "bb"),
                Tuple2.of("c", "cc"),
                Tuple2.of("b", "bb2")
        );

        stream1
                .keyBy(t ->t.f0)
                .connect(stream2.keyBy(t ->t.f0))
                .process(new CoProcessFunction<Tuple2<String, Integer>, Tuple2<String, String>, String>() {

                    private ListState<Tuple2<String,Integer>> listState1;
                    private ListState<Tuple2<String,String>> listState2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        listState1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("listState1", Types.TUPLE(Types.STRING, Types.INT)));
                        listState2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, String>>("listState2",Types.TUPLE(Types.STRING,Types.STRING)));


                    }

                    @Override
                    public void processElement1(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        listState1.add(value);
                        StringBuilder list2String = new StringBuilder();
                        StringBuilder joinResult = new StringBuilder();
                        for(Tuple2<String,String> t2:listState2.get()){
                            list2String.append(t2+",");
                            joinResult.append(value.f1 +" -> " +t2.f1+"  ");
                        }

                        out.collect("ListState2里有：" + list2String + " join结果："+joinResult);
                    }

                    @Override
                    public void processElement2(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
                        listState2.add(value);
                        StringBuilder list1String = new StringBuilder();
                        StringBuilder joinResult = new StringBuilder();
                        for(Tuple2<String,Integer> t1:listState1.get()){
                            list1String.append(t1+",");
                            joinResult.append(t1.f1 +" -> " + value.f1+"  ");
                        }

                        out.collect("ListState1里有：" + list1String + " join结果："+joinResult);
                    }
                })
                .print();

        System.out.println(env.getExecutionPlan());
        env.execute();




    }
}
