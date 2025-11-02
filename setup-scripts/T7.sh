#!/bin/bash
###############################################################################
# 实验7：水位线中迟到数据的处理
###############################################################################

echo "========================================"
echo "实验7：水位线与迟到数据处理"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "错误: JAR 文件不存在"
    exit 1
fi

echo ""
echo "【步骤1】使用命名管道启动 Socket 数据源并自动发送测试数据："

FIFO=/tmp/flink_watermark_fifo_$$
rm -f "${FIFO}" || true
mkfifo "${FIFO}"

# 启动 nc 监听 9999，将 FIFO 内容发送到客户端
( nc -l 9999 < "${FIFO}" ) &
NC_PID=$!
sleep 1

# 后台提交 Flink 作业
FLINK_RUN_LOG="${FLINK_HOME}/log/run-watermark-late-$(date +%s).log"
nohup ${FLINK_HOME}/bin/flink run \
    -c myjob.WatermarkLateDataHandling \
    $JAR_FILE \
    --host master --port 9999 >"${FLINK_RUN_LOG}" 2>&1 &
FLINK_PID=$!

# 写入测试数据到 FIFO 并等待
printf "s1,1,1\ns1,5,5\ns1,13,13\ns1,6,6\ns1,3,3\ns1,15,15\ns1,4,4\n" > "${FIFO}"
sleep 8

# 关闭 nc 并清理
kill ${NC_PID} 2>/dev/null || true
wait ${NC_PID} 2>/dev/null || true
rm -f "${FIFO}"

cat "${FLINK_RUN_LOG}"

echo "预期输出："
echo "  - 窗口[0,10)包含的数据"
echo "  - 关窗后的迟到数据提示"
echo "  - 水位线生成信息"
echo "========================================"
