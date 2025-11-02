#!/bin/bash
###############################################################################
# 实验4：车辆种类数量统计
###############################################################################

echo "========================================"
echo "实验4：使用 ProcessWindowFunction 统计车辆种类数量"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "错误: JAR 文件不存在"
    exit 1
fi

echo ""
echo "【步骤1】启动 Socket 数据源："
echo "  nc -lk 9999"

FIFO=/tmp/flink_vehicle_fifo_$$
rm -f "${FIFO}" || true
mkfifo "${FIFO}"

# 启动 nc 监听 9999，将 FIFO 内容发送到客户端
( nc -l 9999 < "${FIFO}" ) &
NC_PID=$!
sleep 1

# 后台提交 Flink 作业
FLINK_RUN_LOG="${FLINK_HOME}/log/run-vehicle-count-$(date +%s).log"
nohup ${FLINK_HOME}/bin/flink run \
    -c myjob.VehicleCategoryCountProcessWindow \
    $JAR_FILE \
    --host master --port 9999 --window 10 >"${FLINK_RUN_LOG}" 2>&1 &
FLINK_PID=$!

# 写入测试数据到 FIFO 并等待
printf "卡车 10\n燃油车 20\n电动车 20\n摩托车 10\n摩托车 30\n卡车 15\n燃油车 20\n" > "${FIFO}"
sleep 8

# 关闭 nc 并清理
kill ${NC_PID} 2>/dev/null || true
wait ${NC_PID} 2>/dev/null || true
rm -f "${FIFO}"

cat "${FLINK_RUN_LOG}"

echo "预期输出：每个窗口内各车辆种类的总数量及窗口时间"
echo "========================================"
