package com.tqy.sideOutPut;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.math.Ordering;

public class SideOutPutDemo2 {
    public static void main(String[] args) throws Exception {
        OutputTag<String> late = new OutputTag<String>("late"){};

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<String> result = env
                .addSource(new SourceFunction<Tuple2<String, Long>>() {
                    @Override
                    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
                        ctx.collectWithTimestamp(Tuple2.of("hello world", 1000L), 1000L);
                        ctx.emitWatermark(new Watermark(999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello bigdata", 2000L), 2000L);
                        ctx.emitWatermark(new Watermark(1999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello flink", 5000L), 5000L);
                        ctx.emitWatermark(new Watermark(4999L));
                        ctx.collectWithTimestamp(Tuple2.of("hello test", 4000L), 4000L);
                    }

                    @Override
                    public void cancel() {

                    }
                })
                .keyBy(r -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("late"){})
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("窗口中共有 " + elements.spliterator().getExactSizeIfKnown() + " 个元素");
                    }
                });

        result.print();
        result.getSideOutput(new OutputTag<Tuple2<String, Long>>("late"){}).map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                return "迟到的元素："+value;
            }
        }).print();

        env.execute();


    }
}
