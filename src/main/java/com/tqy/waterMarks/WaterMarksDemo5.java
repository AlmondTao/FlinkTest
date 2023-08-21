package com.tqy.waterMarks;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

//自定义水位线生成逻辑
public class WaterMarksDemo5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .setParallelism(1)
                .socketTextStream("192.168.2.100",9999)
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] arr = value.split(" ");
                        return Tuple2.of(arr[0], Long.parseLong(arr[1]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(new CustomWatermarkGenerator())
                .process(new ProcessFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        long currentWatermark = ctx.timerService().currentWatermark();
                        out.collect("接收到数据："+value+" ，当前水位线为："+currentWatermark);
                    }
                })
                .print();


        env.getConfig().setAutoWatermarkInterval(2000L);

        env.execute();

    }

    public static class CustomWatermarkGenerator implements WatermarkStrategy<Tuple2<String,Long>>{

        @Override
        public TimestampAssigner<Tuple2<String, Long>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new TimestampAssigner<Tuple2<String, Long>>() {
                @Override
                public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                    return element.f1;
                }
            };
        }

        @Override
        public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            WatermarkGenerator<Tuple2<String, Long>> watermarkGenerator = new WatermarkGenerator<Tuple2<String, Long>>() {
                private Long bound = 5000L;
                private Long maxTs = -Long.MAX_VALUE + bound + 1;

                @Override
                public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
                    maxTs = Math.max(maxTs,event.f1);
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    //发送水位线
                    output.emitWatermark(new Watermark(maxTs - bound - 1));
                }
            };

            return watermarkGenerator;
        }
    }

}
