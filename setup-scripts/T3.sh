#!/bin/bash
###############################################################################
# 实验3：订单销售额窗口聚合
###############################################################################

echo "========================================"
echo "实验3：使用 AggregateFunction 计算订单销售额"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR_FILE" ]; then
    echo "错误: JAR 文件不存在"
    exit 1
fi

echo ""
echo "【步骤1】启动 Socket 数据源："
echo "  nc -lk 9999"
# 使用命名管道 (FIFO) 作为数据源：
# 原理：启动一个后台的 nc 监听进程，它会在客户端连接时从 FIFO 读取数据并发送。
# 然后把测试数据写入 FIFO（写操作会在没有读者时阻塞，直到 Flink 连接并开始读取）。

FIFO=/tmp/flink_socket_fifo_$$
rm -f "${FIFO}" || true
mkfifo "${FIFO}"

# 启动 nc 监听 master:9999，并把 FIFO 的内容传入（后台运行）
( nc -l 9999 < "${FIFO}" ) &
NC_PID=$!

# 给 nc 少许时间完成监听启动
sleep 1

# 使用 nohup 在后台启动 Flink 作业（将输出重定向到 Flink log 目录下的文件）
FLINK_RUN_LOG="${FLINK_HOME}/log/run-order-sales-$(date +%s).log"
nohup ${FLINK_HOME}/bin/flink run \
    -c myjob.OrderSalesAggregate \
    $JAR_FILE \
    --host master --port 9999 --window 10 >"${FLINK_RUN_LOG}" 2>&1 &
FLINK_PID=$!

# 将测试数据写入 FIFO（写入将在 Flink 连接并读取时被消费），然后等待 5 秒
printf "s10235,towel,2\ns10236,toothbrush,3\ns10237,toothbrush,7\ns10238,instantnoodles,5\ns10239,towel,3\ns10240,toothbrush,3\ns10241,instantnoodles,3" > "${FIFO}"
sleep 8

# 关闭 nc 并清理 FIFO（保留 Flink 进程在后台运行）
kill ${NC_PID} 2>/dev/null || true
wait ${NC_PID} 2>/dev/null || true
rm -f "${FIFO}"

cat "${FLINK_RUN_LOG}"

# 注意：FLINK 仍在后台运行，日志写入 ${FLINK_RUN_LOG}

# echo ""
# echo "【步骤2】提交 Flink 作业（10秒滚动窗口）："
# echo ""

# ${FLINK_HOME}/bin/flink run \
#     -c myjob.OrderSalesAggregate \
#     $JAR_FILE \
#     --host master --port 9999 --window 10

# echo ""
# echo "【步骤3】输入测试数据（格式: orderId,product,quantity）："
# echo "  s10235,towel,2"
# echo "  s10236,toothbrush,3"
# echo "  s10237,toothbrush,7"
# echo "  s10238,instantnoodles,5"
# echo "  s10239,towel,3"
# echo "  s10240,toothbrush,3"
# echo "  s10241,instantnoodles,3"
# echo ""
# echo "价格表: towel=10, toothbrush=5, instantnoodles=3"
# echo "预期输出：每个窗口内各产品的总销售额"
# echo "========================================"
