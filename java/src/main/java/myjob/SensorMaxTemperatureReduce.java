package myjob;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 实验2：使用窗口计算函数 ReduceFunction 统计传感器温度最大值（示例版）
 *
 * 说明：
 * - 功能：按传感器 ID 在固定长度的处理时间滚动窗口内计算最大温度。
 * - 技术点：使用 ReduceFunction 做增量聚合，避免在窗口结束时对所有元素做全量遍历，从而降低内存开销。
 * - 输入格式：sensorId temperature （空格分隔），例如: "sensor_1 36.5"
 * - 窗口类型为 Processing Time 的滚动窗口，窗口长度可通过 --window 参数指定（秒）。
 *
 * 注：注释说明每步输入/输出与意图，便于现场演示和阅读。
 */
public class SensorMaxTemperatureReduce {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;
        int windowSec = 5;
        // 命令行参数解析：支持 --host --port --window 覆盖默认值
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
            if ("--window".equals(args[i]))
                windowSec = Integer.parseInt(args[i + 1]);
        }

        // 获取 Flink 流处理环境并设置并行度为 1，便于观察输出顺序
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 socket 文本流作为数据源，便于通过 nc/netcat 或 telnet 注入样例数据
        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        // 解析输入行为 (sensorId, temperature) 的二元组
        DataStream<Tuple2<String, Double>> parsed = lines
                // 过滤空行，保证下游不接收空数据
                .filter(s -> s != null && !s.trim().isEmpty())
                // 将每行按空白分割并解析：第一列为传感器 ID，第二列为温度值（浮点数）
                .map(s -> {
                    // 简单解析：按空白分割，第一列为 sensorId，第二列为温度
                    String[] parts = s.trim().split("\\s+");
                    String id = parts[0];
                    double temp = Double.parseDouble(parts[1]);
                    return Tuple2.of(id, temp);
                })
                // 明确返回类型，避免泛型擦除导致的类型推断问题
                .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Double>>() {
                });

        // 按 sensorId 分组，使用固定长度的滚动窗口，并通过 ReduceFunction 进行增量聚合（计算最大值）
        DataStream<Tuple2<String, Double>> maxPerWindow = parsed
                // keyBy：按 Tuple2 的 f0（sensorId）进行分区，保证同一传感器的数据到同一并行实例
                .keyBy(t -> t.f0)
                // 使用处理时间滚动窗口，长度由 windowSec 指定
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSec)))
                // ReduceFunction：以增量方式比较并保留更大的温度值
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> v1, Tuple2<String, Double> v2) {
                        // 输入：两个 (sensorId, temp) 元素
                        // 输出：保留相同 sensorId，温度取较大者，实现窗口内的最大值计算
                        return Tuple2.of(v1.f0, Math.max(v1.f1, v2.f1));
                    }
                });

        // 将每个窗口的最大温度打印到控制台
        maxPerWindow.print();
        env.execute("T2");
    }
}
