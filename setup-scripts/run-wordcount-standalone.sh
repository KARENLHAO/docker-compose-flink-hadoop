#!/bin/bash
# 运行 Flink WordCount 示例（Standalone 模式）

echo "===================================="
echo "运行 Flink WordCount 示例"
echo "Flink 模式: Standalone"
echo "===================================="

# 创建测试数据
echo "创建测试数据..."
mkdir -p /tmp/flink-test
cat > /tmp/flink-test/words.txt <<EOF
Apache Flink is a framework and distributed processing engine
for stateful computations over unbounded and bounded data streams
Flink has been designed to run in all common cluster environments
perform computations at in-memory speed and at any scale
EOF

# 上传到 HDFS
echo "上传测试数据到 HDFS..."
hdfs dfs -mkdir -p /flink-test
hdfs dfs -rm -f /flink-test/words.txt
# 如结果目录已存在，递归删除以避免 NO_OVERWRITE 报错
hdfs dfs -rm -r -f /flink-test/wordcount-result-standalone
hdfs dfs -put -f /tmp/flink-test/words.txt /flink-test/

# 运行 WordCount
echo "运行 WordCount 任务..."
${FLINK_HOME}/bin/flink run \
    ${FLINK_HOME}/examples/batch/WordCount.jar \
    --input hdfs://master:9000/flink-test/words.txt \
    --output hdfs://master:9000/flink-test/wordcount-result-standalone

# 等待任务完成
sleep 5

# 查看结果
echo ""
echo "===================================="
echo "WordCount 结果："
echo "===================================="
# 先列出生成的结果文件，然后输出内容
hdfs dfs -ls /flink-test/wordcount-result-standalone || true
hdfs dfs -cat /flink-test/wordcount-result-standalone/part-* || true

echo ""
echo "===================================="
echo "任务完成！"
echo "===================================="
