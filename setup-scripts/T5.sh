#!/bin/bash
###############################################################################
# 实验5：水传感器最大水位值分析
###############################################################################

echo "========================================"
echo "实验5：增量聚合 + 全窗口函数输出最大水位"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "错误: JAR 文件不存在"
    exit 1
fi

echo ""
echo "【步骤1】使用命名管道启动 Socket 数据源并自动发送测试数据："

FIFO=/tmp/flink_water_fifo_$$
rm -f "${FIFO}" || true
mkfifo "${FIFO}"

# 启动 nc 监听 9999，将 FIFO 内容发送到客户端
( nc -l 9999 < "${FIFO}" ) &
NC_PID=$!
sleep 1

# 后台提交 Flink 作业
FLINK_RUN_LOG="${FLINK_HOME}/log/run-water-sensor-$(date +%s).log"
nohup ${FLINK_HOME}/bin/flink run \
    -c myjob.WaterSensorMaxLevelAggAndProcess \
    $JAR_FILE \
    --host master --port 9999 --window 10 >"${FLINK_RUN_LOG}" 2>&1 &
FLINK_PID=$!

# 写入测试数据到 FIFO 并等待
printf "s1,20\ns1,30\ns1,25\ns2,10\ns2,15\ns1,40\ns1,35\ns2,10\ns2,5\n" > "${FIFO}"
sleep 8

# 关闭 nc 并清理
kill ${NC_PID} 2>/dev/null || true
wait ${NC_PID} 2>/dev/null || true
rm -f "${FIFO}"

cat "${FLINK_RUN_LOG}"

echo "预期输出：每个传感器在窗口内的最大水位值及窗口起止时间"
echo "========================================"
