#!/bin/bash
# 启动 Hadoop 集群脚本

echo "===================================="
echo "启动 Hadoop 集群"
echo "===================================="

# 检查是否是 master 节点
if [ "$NODE_TYPE" != "master" ]; then
    echo "此脚本只能在 master 节点运行"
    exit 1
fi

# 首次启动需要格式化 NameNode
if [ ! -d "/home/hadoop/data/namenode/current" ]; then
    echo "格式化 HDFS NameNode..."
    hdfs namenode -format -force
fi

# 启动 HDFS
echo "启动 HDFS..."
start-dfs.sh

# 等待 HDFS 启动
sleep 10

# 创建必要的 HDFS 目录
echo "创建 HDFS 目录..."
hdfs dfs -mkdir -p /user/hadoop
hdfs dfs -mkdir -p /flink
hdfs dfs -mkdir -p /flink/completed-jobs
hdfs dfs -mkdir -p /tmp

# 启动 YARN
echo "启动 YARN..."
start-yarn.sh

# 启动 MapReduce Job History Server
echo "启动 Job History Server..."
mapred --daemon start historyserver

echo "===================================="
echo "Hadoop 集群启动完成！"
echo "Web UI 访问地址："
echo "  HDFS NameNode: http://localhost:9870"
echo "  YARN ResourceManager: http://localhost:8088"
echo "  Job History Server: http://localhost:19888"
echo "===================================="

# 检查集群状态
echo ""
echo "集群状态："
hdfs dfsadmin -report
echo ""
yarn node -list
