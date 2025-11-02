#!/bin/bash
# 一键初始化所有节点的配置
# 本脚本会自动检测节点类型并执行相应配置

set -e

echo "============================================"
echo "开始一键初始化 Hadoop & Flink 集群"
echo "============================================"

CURRENT_NODE=$(hostname)
echo "当前节点: ${CURRENT_NODE}"

# 确保在 /workspace 目录
cd /workspace

# 1. 在所有节点上配置 Hadoop
echo ""
echo ">>> 步骤 1/4: 配置 Hadoop..."
chmod +x setup-scripts/*.sh
./setup-scripts/setup-hadoop.sh

# 2. 在所有节点上配置 Flink Standalone
echo ""
echo ">>> 步骤 2/4: 配置 Flink Standalone..."
./setup-scripts/setup-flink-standalone.sh

# 3. 仅在 master 节点配置 Flink YARN
if [ "${CURRENT_NODE}" = "master" ]; then
    echo ""
    echo ">>> 步骤 3/4: 配置 Flink YARN..."
    ./setup-scripts/setup-flink-yarn.sh
    
    # 4. 从 master 同步配置到 worker 节点
    echo ""
    echo ">>> 步骤 4/4: 同步配置到 worker 节点..."
    
    echo "等待 worker 节点 SSH 服务启动..."
    sleep 5
    
    
    
    # 下载并安装 Flink Hadoop 依赖
    echo ""
    echo ">>> 下载 Flink Hadoop 文件系统支持..."
    FLINK_HADOOP_JAR="flink-shaded-hadoop-2-uber-2.8.3-10.0.jar"
    if [ ! -f "${FLINK_HOME}/lib/${FLINK_HADOOP_JAR}" ]; then
        cd /tmp
        wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/${FLINK_HADOOP_JAR}
        cp ${FLINK_HADOOP_JAR} ${FLINK_HOME}/lib/
        echo "Flink Hadoop JAR 已安装到 master"
        
        # 同步到 worker 节点
        scp -o StrictHostKeyChecking=no ${FLINK_HOME}/lib/${FLINK_HADOOP_JAR} worker1:${FLINK_HOME}/lib/
        echo "Flink Hadoop JAR 已同步到 worker1"
        
        scp -o StrictHostKeyChecking=no ${FLINK_HOME}/lib/${FLINK_HADOOP_JAR} worker2:${FLINK_HOME}/lib/
        echo "Flink Hadoop JAR 已同步到 worker2"
    else
        echo "Flink Hadoop JAR 已存在，跳过下载"
    fi
    
    # 格式化 HDFS (首次部署)
    echo ""
    echo ">>> 格式化 HDFS NameNode..."
    if [ ! -d "/home/hadoop/data/namenode/current" ]; then
        hdfs namenode -format -force
        echo "HDFS 格式化完成"
    else
        echo "HDFS 已格式化，跳过"
    fi
    
    echo ""
    echo "============================================"
    echo "Master 节点初始化完成！"
    echo ""
    echo "下一步操作:"
    echo "1. 启动集群: ./setup-scripts/start-hadoop.sh"
    echo "2. 验证 HDFS: hdfs dfsadmin -report"
    echo "3. 验证 YARN: yarn node -list"
    echo "4. 启动 Flink: ./setup-scripts/start-flink-standalone.sh"
    echo "5. 运行测试: ./setup-scripts/run-wordcount-standalone.sh"
    echo ""
    echo "Web UI 访问地址:"
    echo "  HDFS NameNode: http://localhost:9870"
    echo "  YARN ResourceManager: http://localhost:8088"
    echo "  Flink Dashboard: http://localhost:8081"
    echo "============================================"
else
    echo ""
    echo ">>> Worker 节点配置完成，等待 master 节点同步..."
    echo "============================================"
    echo "Worker 节点 (${CURRENT_NODE}) 初始化完成！"
    echo "请在 master 节点执行完整初始化"
    echo "============================================"
fi
