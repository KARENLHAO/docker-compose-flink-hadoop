#!/bin/bash
# Flink Standalone 模式配置脚本

echo "===================================="
echo "开始配置 Flink Standalone 集群"
echo "===================================="

# 配置 flink-conf.yaml
cat > ${FLINK_HOME}/conf/flink-conf.yaml <<EOF
# JobManager 配置
jobmanager.rpc.address: master
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 1600m

# TaskManager 配置
taskmanager.memory.process.size: 1728m
taskmanager.numberOfTaskSlots: 2

# 高可用配置（可选）
# high-availability: zookeeper
# high-availability.storageDir: hdfs:///flink/ha/
# high-availability.zookeeper.quorum: master:2181

# Web UI 配置
rest.port: 8081
rest.address: master

# 历史服务器配置
jobmanager.archive.fs.dir: hdfs:///flink/completed-jobs/
historyserver.web.address: master
historyserver.web.port: 8082
historyserver.archive.fs.dir: hdfs:///flink/completed-jobs/
historyserver.archive.fs.refresh-interval: 10000

# 其他配置
io.tmp.dirs: /home/hadoop/data/tmp/flink
classloader.resolve-order: child-first
EOF

# 配置 masters 文件
cat > ${FLINK_HOME}/conf/masters <<EOF
master:8081
EOF

# 配置 workers 文件
cat > ${FLINK_HOME}/conf/workers <<EOF
worker1
worker2
EOF

echo "===================================="
echo "Flink Standalone 模式配置完成！"
echo "===================================="
