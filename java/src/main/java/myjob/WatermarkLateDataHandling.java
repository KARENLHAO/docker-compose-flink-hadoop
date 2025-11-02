package myjob;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 实验7：水位线中迟到数据的处理
 * 输入：sensorId,timestamp,value （timestamp 单位：秒）
 * 要求：自定义水位线生成器；事件时间窗口 10s；允许迟到 2s；并打印窗口信息与迟到数据。
 */
public class WatermarkLateDataHandling {

    public static final OutputTag<Tuple3<String, Long, Integer>> LATE_TAG = new OutputTag<Tuple3<String, Long, Integer>>(
            "late") {
    };

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        DataStream<Tuple3<String, Long, Integer>> parsed = lines
                .filter(s -> s != null && !s.trim().isEmpty())
                .map(s -> s.trim().split(","))
                .filter(arr -> arr.length >= 3)
                .map(arr -> Tuple3.of(arr[0].trim(), Long.parseLong(arr[1].trim()) * 1000L,
                        Integer.parseInt(arr[2].trim())))
                .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple3<String, Long, Integer>>() {
                });

        WatermarkStrategy<Tuple3<String, Long, Integer>> wmStrategy = new CustomWatermarkStrategy();

        DataStream<Tuple3<String, Long, Integer>> withWm = parsed
                .assignTimestampsAndWatermarks(wmStrategy);

        org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<String> main = withWm
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(LATE_TAG)
                .process(new ProcessWindowFunction<Tuple3<String, Long, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx, Iterable<Tuple3<String, Long, Integer>> elements,
                            Collector<String> out) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("key=").append(key)
                                .append(" 窗口[")
                                .append(formatTs(ctx.window().getStart())).append(",")
                                .append(formatTs(ctx.window().getEnd())).append(")包含");
                        sb.append("数据===>[");
                        for (Tuple3<String, Long, Integer> e : elements) {
                            sb.append("(").append(e.f0).append(",").append(e.f1 / 1000).append(",").append(e.f2)
                                    .append(")").append(", ");
                        }
                        sb.append("]");
                        out.collect(sb.toString());
                    }
                });

        main.print();
        // 打印迟到数据
        main.getSideOutput(LATE_TAG).map(t -> "关窗后的迟到数据> (" + t.f0 + "," + (t.f1 / 1000) + "," + t.f2 + ")").print();

        env.execute("T7");
    }

    static String formatTs(long ts) {
        return new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(ts));
    }

    static class CustomWatermarkStrategy implements WatermarkStrategy<Tuple3<String, Long, Integer>> {
        @Override
        public TimestampAssigner<Tuple3<String, Long, Integer>> createTimestampAssigner(
                TimestampAssignerSupplier.Context ctx) {
            return (element, recordTimestamp) -> element.f1; // event time from input
        }

        @Override
        public WatermarkGenerator<Tuple3<String, Long, Integer>> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context ctx) {
            return new WatermarkGenerator<Tuple3<String, Long, Integer>>() {
                private long maxTs = Long.MIN_VALUE + 1;
                private final long outOfOrderness = 0L; // 以示例为主，打印更直观；可改为 2s

                @Override
                public void onEvent(Tuple3<String, Long, Integer> event, long eventTimestamp, WatermarkOutput output) {
                    maxTs = Math.max(maxTs, event.f1);
                    output.emitWatermark(new Watermark(maxTs - outOfOrderness - 1));
                    System.out.println("Emit watermark: " + formatTs(maxTs - outOfOrderness - 1));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(maxTs - outOfOrderness - 1));
                }
            };
        }
    }
}
