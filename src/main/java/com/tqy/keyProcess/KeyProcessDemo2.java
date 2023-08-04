package com.tqy.keyProcess;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

//利用状态 实现时间窗口
public class KeyProcessDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .addSource(new SourceFunction<Integer>() {

                    private Random random = new Random();
                    private Boolean running = true;

                    @Override
                    public void run(SourceContext<Integer> ctx) throws Exception {

                        while (running){
                            ctx.collect(random.nextInt(100));
                            Thread.sleep(1000L);
                        }

                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(i -> true)
                .process(new KeyedProcessFunction<Boolean, Integer, String>() {
                    private ValueState<Long> timerTimeStamp;

                    private ValueState<Tuple2<Long,Long>> sumAndCount;

                    private Long timerLength = 10 *1000L;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        sumAndCount = getRuntimeContext().getState(new ValueStateDescriptor("sumAndCount", Types.TUPLE(Types.LONG,Types.LONG)));
                        timerTimeStamp = getRuntimeContext().getState(new ValueStateDescriptor("timerTimeStamp", Types.LONG));

                    }

                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        if (timerTimeStamp.value() == null){
                            long currentTime = ctx.timerService().currentProcessingTime();
                            timerTimeStamp.update(currentTime+timerLength);
                            ctx.timerService().registerProcessingTimeTimer(currentTime+timerLength);
                        }
                        if (sumAndCount.value() == null){
                            sumAndCount.update(Tuple2.of(value.longValue(),1L));
                        } else {
                            Tuple2<Long, Long> tuple2 = sumAndCount.value();
                            sumAndCount.update(Tuple2.of(tuple2.f0+value,tuple2.f1+1));
                        }
                        out.collect("数据到达："+value);


                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        Tuple2<Long, Long> tuple2 = sumAndCount.value();
                        //不是百分百10个总数
                        out.collect("=========================================\n" +
                                    "定时器："+ new Timestamp(timerTimeStamp.value())+"启动。\n"+
                                    "定时器计算结果 总和："+tuple2.f0+", 总数："+tuple2.f1+", 平均值:"+(double)tuple2.f0/ tuple2.f1+"\n"+
                                    "=========================================\n"
                        );
                        timerTimeStamp.clear();
                        sumAndCount.clear();
                    }
                }).print();
        

        env.execute();
    }
}
