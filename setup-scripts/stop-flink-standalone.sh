#!/bin/bash
# 停止 Flink Standalone 集群脚本

echo "===================================="
echo "停止 Flink Standalone 集群"
echo "===================================="

# 停止 History Server
echo "停止 Flink History Server..."
${FLINK_HOME}/bin/historyserver.sh stop

# 停止 Flink 集群
echo "停止 Flink 集群..."
${FLINK_HOME}/bin/stop-cluster.sh

echo "===================================="
echo "Flink Standalone 集群已停止"
echo "===================================="
