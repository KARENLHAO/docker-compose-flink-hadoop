#!/bin/bash
# Flink on YARN 模式配置脚本

echo "===================================="
echo "开始配置 Flink on YARN"
echo "===================================="

# 配置 flink-conf.yaml for YARN
cat > ${FLINK_HOME}/conf/flink-conf-yarn.yaml <<EOF
# YARN 配置
# 当使用 YARN 模式时，可以使用此配置文件

# JobManager 配置
jobmanager.memory.process.size: 1600m

# TaskManager 配置
taskmanager.memory.process.size: 1728m
taskmanager.numberOfTaskSlots: 2

# 其他配置
io.tmp.dirs: /home/hadoop/data/tmp/flink
classloader.resolve-order: child-first

# Web UI 配置
rest.port: 8081

# YARN 应用配置
yarn.application.name: Flink Application
yarn.application.queue: default
EOF

# 设置 Hadoop classpath for Flink
echo "export HADOOP_CLASSPATH=\$(hadoop classpath)" >> ~/.bashrc

# 创建必要的 HDFS 目录
echo "创建 HDFS 目录..."
# 注意：需要在 HDFS 启动后执行
# hdfs dfs -mkdir -p /flink
# hdfs dfs -mkdir -p /flink/completed-jobs

echo "===================================="
echo "Flink on YARN 配置完成！"
echo "使用前请确保："
echo "1. HDFS 已启动"
echo "2. YARN 已启动"
echo "3. 执行: export HADOOP_CLASSPATH=\$(hadoop classpath)"
echo "===================================="
