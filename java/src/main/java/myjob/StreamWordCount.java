package myjob;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Flink 流式 WordCount 应用程序（教学版）
 *
 * 功能说明：
 * - 演示 Flink 流式 API 的基本用法：从 Socket 读取流、flatMap 分词、keyBy 分组、window 聚合以及汇报结果。
 * - 本例使用 Processing Time 窗口（滚动窗口），便于课堂演示。生产环境通常使用 Event Time。
 *
 * 使用方法（课堂环境）：
 * 1. 启动 Socket 数据源: nc -lk 9999
 * 2. 运行程序，指定 --host 和 --port
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 参数解析
        String host = "localhost";
        int port = 9999;

        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        } else if (args.length == 1 && args[0].equals("--help")) {
            System.out.println("Usage: StreamWordCount --host <hostname> --port <port>");
            System.out.println("Example: StreamWordCount --host localhost --port 9999");
            return;
        }

        // 解析命名参数
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals("--host")) {
                host = args[i + 1];
            } else if (args[i].equals("--port")) {
                port = Integer.parseInt(args[i + 1]);
            }
        }

        System.out.println("===========================================");
        System.out.println("Flink Stream WordCount Application Started");
        System.out.println("Reading from: " + host + ":" + port);
        System.out.println("===========================================");

        // 1. 创建流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度
        env.setParallelism(2);

        // 2. 从Socket读取数据流
        DataStream<String> textStream = env.socketTextStream(host, port, "\n");

        // 3. 数据处理：分词、计数、聚合
        DataStream<Tuple2<String, Integer>> wordCounts = textStream
                // 分词
                .flatMap(new Tokenizer())
                // 按单词分组
                .keyBy(value -> value.f0)
                // 5秒滚动窗口
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // 求和
                .sum(1);

        // 4. 输出结果
        wordCounts.print();

        // 5. 执行任务
        env.execute("Flink Stream WordCount");
    }

    /**
     * 分词器：将一行文本拆分成单词（教学版）
     * 注意：流式处理中，flatMap 是常见的“展开”算子，用于把一条记录拆成多条记录。
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // 转小写并分割
            String[] words = value.toLowerCase().split("\\W+");

            // 输出每个单词
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
