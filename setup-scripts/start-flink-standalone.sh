#!/bin/bash
# 启动 Flink Standalone 集群脚本

echo "===================================="
echo "启动 Flink Standalone 集群"
echo "===================================="

# 检查是否是 master 节点
if [ "$NODE_TYPE" != "master" ]; then
    echo "此脚本只能在 master 节点运行"
    exit 1
fi

# 启动 Flink 集群
echo "启动 Flink 集群..."
${FLINK_HOME}/bin/start-cluster.sh

# 等待启动
sleep 5

# 启动 History Server
echo "启动 Flink History Server..."
${FLINK_HOME}/bin/historyserver.sh start

echo "===================================="
echo "Flink Standalone 集群启动完成！"
echo "Web UI 访问地址："
echo "  Flink Dashboard: http://localhost:8081"
echo "  Flink History Server: http://localhost:8082"
echo "===================================="
