package myjob;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 实验5：增量聚合 + 全窗口函数输出最大水位和窗口信息
 * 输入：sensorId,level
 */
public class WaterSensorMaxLevelAggAndProcess {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;
        int windowSec = 10;
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
            if ("--window".equals(args[i]))
                windowSec = Integer.parseInt(args[i + 1]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        DataStream<Tuple2<String, Integer>> parsed = lines
                .filter(s -> s != null && !s.trim().isEmpty())
                .map(s -> s.trim().split(","))
                .filter(arr -> arr.length >= 2)
                .map(arr -> Tuple2.of(arr[0].trim(), Integer.parseInt(arr[1].trim())))
                .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Integer>>() {
                });

        DataStream<String> result = parsed
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSec)))
                .aggregate(new MaxAgg(), new WithWindowInfo());

        result.print();
        env.execute("T5");
    }

    static class MaxAgg implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return Integer.MIN_VALUE;
        }

        @Override
        public Integer add(Tuple2<String, Integer> value, Integer acc) {
            return Math.max(acc, value.f1);
        }

        @Override
        public Integer getResult(Integer acc) {
            return acc;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return Math.max(a, b);
        }
    }

    static class WithWindowInfo extends ProcessWindowFunction<Integer, String, String, TimeWindow> {
        @Override
        public void process(String key, Context ctx, Iterable<Integer> elements, Collector<String> out) {
            int max = elements.iterator().next();
            long start = ctx.window().getStart();
            long end = ctx.window().getEnd();
            out.collect(String.format("sensor=%s window=[%d,%d) max=%d", key, start, end, max));
        }
    }
}
