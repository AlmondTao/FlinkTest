package com.tqy.sideOutPut;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutDemo1 {
    public static void main(String[] args) throws Exception {

        OutputTag<String> outputTag = new OutputTag<String>("late-element") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<String> process = env
                .setParallelism(1)
                .addSource(new SourceFunction<Tuple2<String, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                        ctx.collectWithTimestamp(Tuple2.of("hello world", 1000L), 1000L);
                        ctx.emitWatermark(new Watermark(999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello bigdata", 2000L), 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello flink", 4000L), 4000L);
                        ctx.emitWatermark(new Watermark(3999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello test", 3000L), 3000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        if (value.f1 < currentWatermark) {
                            ctx.output(outputTag, "迟到的元素：" + value);
                        } else {
                            out.collect("接收到元素:" + value);
                        }
                    }
                });
        process.print();
        DataStream<String> sideOutput = process.getSideOutput(outputTag);
        sideOutput.print();

        env.execute();
    }
}
