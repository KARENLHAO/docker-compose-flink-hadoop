#!/bin/bash
###############################################################################
# 实验2：传感器温度最大值统计
###############################################################################

echo "========================================"
echo "实验2：使用 ReduceFunction 统计传感器温度最大值"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "错误: JAR 文件不存在"
    exit 1
fi

echo ""
echo "【步骤1】使用命名管道启动 Socket 数据源并自动发送测试数据："

FIFO=/tmp/flink_sensor_fifo_$$
rm -f "${FIFO}" || true
mkfifo "${FIFO}"

# 启动 nc 监听 9999，将 FIFO 内容发送到客户端
( nc -l 9999 < "${FIFO}" ) &
NC_PID=$!
sleep 1

# 后台提交 Flink 作业
FLINK_RUN_LOG="${FLINK_HOME}/log/run-sensor-temp-$(date +%s).log"
nohup ${FLINK_HOME}/bin/flink run \
    -c myjob.SensorMaxTemperatureReduce \
    $JAR_FILE \
    --host master --port 9999 --window 5 >"${FLINK_RUN_LOG}" 2>&1 &
FLINK_PID=$!

# 写入测试数据到 FIFO 并等待
printf "s1 2\ns2 3\ns3 4\ns1 6\n" > "${FIFO}"
sleep 8

# 关闭 nc 并清理
kill ${NC_PID} 2>/dev/null || true
wait ${NC_PID} 2>/dev/null || true
rm -f "${FIFO}"

cat "${FLINK_RUN_LOG}"

echo "预期输出：每个窗口内各传感器的最大温度值"
echo "========================================"
