#!/bin/bash
# 运行 Flink WordCount 示例（YARN 模式）

echo "===================================="
echo "运行 Flink WordCount 示例"
echo "Flink 模式: YARN"
echo "===================================="

# 设置 Hadoop classpath
export HADOOP_CLASSPATH=$(hadoop classpath)

# 创建测试数据（如果不存在）
if ! hdfs dfs -test -e /flink-test/words.txt; then
    echo "创建测试数据..."
    mkdir -p /tmp/flink-test
    cat > /tmp/flink-test/words.txt <<EOF
Apache Flink is a framework and distributed processing engine
for stateful computations over unbounded and bounded data streams
Flink has been designed to run in all common cluster environments
perform computations at in-memory speed and at any scale
EOF
    
    echo "上传测试数据到 HDFS..."
    hdfs dfs -mkdir -p /flink-test
    hdfs dfs -put -f /tmp/flink-test/words.txt /flink-test/
fi

# 运行模式选择
echo "选择运行模式："
echo "1. YARN Session 模式（启动一个长期运行的 Flink 会话）"
echo "2. YARN Per-Job 模式（单独为此任务启动集群）"
read -p "请选择 (1 或 2，默认为 2): " MODE

if [ "$MODE" = "1" ]; then
    echo ""
    echo "===================================="
    echo "使用 YARN Session 模式"
    echo "===================================="
    
    # 启动 YARN Session
    echo "启动 YARN Session..."
    ${FLINK_HOME}/bin/yarn-session.sh -d \
        -jm 1024m \
        -tm 1024m \
        -s 2 \
        -nm "Flink Session"
    
    # 等待 Session 启动
    sleep 10
    
    # 提交作业到 Session
    echo "提交 WordCount 任务到 YARN Session..."
    ${FLINK_HOME}/bin/flink run \
        ${FLINK_HOME}/examples/batch/WordCount.jar \
        --input hdfs://master:9000/flink-test/words.txt \
        --output hdfs://master:9000/flink-test/wordcount-result-yarn-session
    
    # 查看结果
    echo ""
    echo "===================================="
    echo "WordCount 结果："
    echo "===================================="
    hdfs dfs -cat /flink-test/wordcount-result-yarn-session
    
    echo ""
    echo "YARN Session 仍在运行，可以提交更多任务"
    echo "停止 Session: echo 'stop' | ${FLINK_HOME}/bin/yarn-session.sh -id <application_id>"
    
else
    echo ""
    echo "===================================="
    echo "使用 YARN Per-Job 模式"
    echo "===================================="
    
    # Per-Job 模式直接运行
    echo "以 Per-Job 模式运行 WordCount 任务..."
    ${FLINK_HOME}/bin/flink run -t yarn-per-job \
        -Dyarn.application.name="Flink WordCount" \
        -Djobmanager.memory.process.size=1024m \
        -Dtaskmanager.memory.process.size=1024m \
        -Dtaskmanager.numberOfTaskSlots=2 \
        ${FLINK_HOME}/examples/batch/WordCount.jar \
        --input hdfs://master:9000/flink-test/words.txt \
        --output hdfs://master:9000/flink-test/wordcount-result-yarn-perjob
    
    # 查看结果
    echo ""
    echo "===================================="
    echo "WordCount 结果："
    echo "===================================="
    hdfs dfs -cat /flink-test/wordcount-result-yarn-perjob
fi

echo ""
echo "===================================="
echo "任务完成！"
echo "可以在 YARN ResourceManager UI 查看任务详情："
echo "http://localhost:8088"
echo "===================================="
