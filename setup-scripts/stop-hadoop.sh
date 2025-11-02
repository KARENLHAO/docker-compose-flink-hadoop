#!/bin/bash
# 停止 Hadoop 集群脚本

echo "===================================="
echo "停止 Hadoop 集群"
echo "===================================="

# 停止 MapReduce Job History Server
echo "停止 Job History Server..."
mapred --daemon stop historyserver

# 停止 YARN
echo "停止 YARN..."
stop-yarn.sh

# 停止 HDFS
echo "停止 HDFS..."
stop-dfs.sh

echo "===================================="
echo "Hadoop 集群已停止"
echo "===================================="
