package myjob;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 实验4：使用窗口计算函数 ProcessWindowFunction 完成车辆种类数量统计
 * 输入格式：<种类> <数量>
 *
 * 说明：
 * - 功能：按车辆种类汇总窗口内的数量总和，输出带窗口起止时间的统计字符串。
 * - 技术点：使用处理时间的滚动窗口 + ProcessWindowFunction，展示如何在窗口层面访问窗口元信息（如 start/end）。
 * - 输入示例： "car 3" 或 "truck 2"（以空白分隔种类与数量）
 */
public class VehicleCategoryCountProcessWindow {

    public static void main(String[] args) throws Exception {
        // 默认连接参数（可通过命令行覆盖）
        String host = "localhost";
        int port = 9999;
        int windowSec = 10;

        // 支持通过 --host --port --window 参数覆盖默认值
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
            if ("--window".equals(args[i]))
                windowSec = Integer.parseInt(args[i + 1]);
        }

        // 获取 Flink 流执行环境并设置并行度为 1，便于观察输出顺序
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 socket 文本流作为数据源，便于通过 nc/netcat 或 telnet 注入样例数据
        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        // 解析输入行，转换为 (category, count) 的二元组
        DataStream<Tuple2<String, Integer>> parsed = lines
                // 过滤空行或空白输入
                .filter(s -> s != null && !s.trim().isEmpty())
                // 将每行按空白分割，取第一个为种类，第二个为数量并解析为整数
                .map(s -> {
                    String[] parts = s.trim().split("\\s+");
                    // 假设输入格式正确：至少包含两个字段
                    String category = parts[0];
                    int count = Integer.parseInt(parts[1]);
                    return Tuple2.of(category, count);
                })
                // 明确返回类型，避免泛型类型擦除导致的推断问题
                .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Integer>>() {
                });

        // 按 category 分组，使用滚动处理时间窗口，窗口内使用 ProcessWindowFunction 做聚合并访问窗口信息
        DataStream<String> windowed = parsed
                // keyBy：按 Tuple2 的 f0（category）进行分区
                .keyBy(t -> t.f0)
                // 使用固定长度的滚动窗口，长度由 windowSec 指定
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSec)))
                // ProcessWindowFunction：在窗口触发时对窗口内所有元素进行遍历并输出带窗口信息的字符串
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements,
                            Collector<String> out) {
                        // 获取窗口的起止时间（处理时间语义）
                        long start = context.window().getStart();
                        long end = context.window().getEnd();

                        // 聚合窗口内的数量总和
                        int sum = 0;
                        for (Tuple2<String, Integer> e : elements) {
                            sum += e.f1;
                        }

                        // 输出示例格式：category=car window=[start,end) total=42
                        // 说明：start 和 end 是毫秒级的时间戳，可以根据需要格式化为可读时间
                        out.collect(String.format("category=%s window=[%d,%d) total=%d", key, start, end, sum));
                    }
                });

        // 将窗口统计结果打印到控制台
        windowed.print();

        env.execute("T4");
    }
}
