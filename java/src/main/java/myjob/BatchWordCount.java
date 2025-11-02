package myjob;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

/**
 * Flink 批处理 WordCount 应用程序（教学版，带详细注释）
 *
 * 功能说明（课堂要点）：
 * - 演示如何使用 Flink 的批处理 API（ExecutionEnvironment）处理静态数据集。
 * - 读取文本文件，对每行进行分词（tokenize），然后统计每个单词出现的次数并输出。
 *
 * 数据格式：任意文本文件，按行读取；分词使用正则 ``\W+``（非字母数字为分隔符）。
 *
 * 输入参数：
 * - --input  : 输入文件路径（默认为 hdfs:///flink-test/input/words.txt）
 * - --output : 输出路径（默认为 hdfs:///flink-test/output/batch-wordcount-result）
 *
 * 课堂讨论点：
 * - ExecutionEnvironment vs StreamExecutionEnvironment 的差异（批处理与流处理）
 * - flatMap 的作用：从一条记录产生 0..N 条新记录
 * - groupBy + sum 的聚合过程
 * - 在没有输出路径时使用 print() 便于本地调试
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 解析参数
        final ParameterTool params = ParameterTool.fromArgs(args);
        
        // 获取输入输出路径
        String inputPath = params.get("input", "hdfs:///flink-test/input/words.txt");
        String outputPath = params.get("output", "hdfs:///flink-test/output/batch-wordcount-result");

        System.out.println("===========================================");
        System.out.println("Flink Batch WordCount Application Started");
        System.out.println("Input:  " + inputPath);
        System.out.println("Output: " + outputPath);
        System.out.println("===========================================");

        // 1. 创建批处理执行环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        
        // 将参数传递给全局配置
        env.getConfig().setGlobalJobParameters(params);

        // 2. 读取文本数据
        DataSet<String> text = env.readTextFile(inputPath);

        // 3. 数据处理：分词、计数、聚合
        DataSet<Tuple2<String, Integer>> counts = text
            // 分词
            .flatMap(new Tokenizer())
            // 按单词分组并求和
            .groupBy(0)
            .sum(1);

        // 4. 输出结果
        if (params.has("output")) {
            counts.writeAsCsv(outputPath, "\n", " ");
        } else {
            counts.print();
        }

        // 5. 执行任务
        env.execute("Flink Batch WordCount");
        
        System.out.println("===========================================");
        System.out.println("Job Completed Successfully!");
        System.out.println("Results saved to: " + outputPath);
        System.out.println("===========================================");
    }

    /**
     * 分词器：将一行文本拆分成单词（Tokenizer 做三件事）
     * 1) 将文本转换为小写，保证统计不区分大小写；
     * 2) 使用正则分割为单词；
     * 3) 对每个非空单词输出 (word, 1) 作为计数基元。
     *
     * 教学提示：可引导学生观察当遇到标点 / 连字符 / 数字时的分词结果，讨论业务场景是否需要更复杂的清洗。
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
