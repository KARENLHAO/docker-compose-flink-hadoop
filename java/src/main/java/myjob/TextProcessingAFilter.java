package myjob;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 实验1：使用多种算子完成 Flink 文本处理
 *
 * 要求：
 * - Socket 数据源
 * - map 转小写
 * - flatMap 分词
 * - filter 仅保留包含字母 "a" 的单词（不区分大小写）
 * - keyBy 按单词分组
 * - reduce 统计每个单词出现次数
 * - print 输出
 *
 */
public class TextProcessingAFilter {

    public static void main(String[] args) throws Exception {
        String host = "master";
        int port = 9999;
        // 支持 --host --port 参数：便于在不同机器或端口上运行测试
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
        }

        // 获取 Flink 流处理环境并设置并行度为 1，方便观察顺序输出
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 一般演示或本地调试时使用 1

        // 使用 socket 文本流作为数据源，便于通过 nc/netcat 或 telnet 输入示例数据
        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        DataStream<Tuple2<String, Integer>> result = lines
                // 1) map: 将整行文本转换为小写，便于后续不区分大小写的处理
                // 输入: String（原始一行文本）
                // 输出: String（小写的一行文本）
                .map((MapFunction<String, String>) String::toLowerCase)

                // 2) flatMap: 将一行文本拆分成若干单词，并为每个单词输出 (word, 1)
                // 输入: String（小写的一行文本）
                // 输出: Tuple2<String, Integer> 多个元素，例如 ("hello",1), ("world",1)
                .flatMap(new Tokenizer())

                // 3) filter: 仅保留包含字母 'a' 的单词（因为已转小写，所以直接判断）
                // 输入/输出类型均为 Tuple2<String,Integer>，不满足条件的元素被丢弃
                .filter(t -> t.f0.contains("a"))

                // 4) keyBy: 按单词本身分组。相同 key 的元素会被送到同一并行实例处理
                // 这里的 key 是 Tuple2 的第一个字段 f0（单词字符串）
                .keyBy(t -> t.f0)

                // 5) reduce: 对相同 key (单词) 的元素做累加，统计出现次数
                // 输入: 两个 Tuple2<String, Integer>
                // 输出: 合并后的 Tuple2<String, Integer>（保持单词，计数相加）
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
                        // v1.f0: 单词字符串；v1.f1 和 v2.f1 是要相加的计数
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });

        // 输出结果到控制台，便于观察处理后的计数
        result.print();

        env.execute("T1");
    }

    // Tokenizer: 将一行文本拆分成单词并输出 (word, 1)
    static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            if (value == null)
                return;
            // 使用非字母数字作为分隔符（\W+），得到若干单词
            String[] words = value.split("\\W+");
            for (String w : words) {
                // 过滤空串并输出 (word, 1)
                if (w != null && !w.isEmpty()) {
                    out.collect(Tuple2.of(w, 1));
                }
            }
        }
    }
}

