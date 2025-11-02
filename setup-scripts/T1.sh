#!/bin/bash
###############################################################################
# 实验1：文本处理 - 统计包含字母a的单词
###############################################################################

echo "========================================"
echo "实验1：使用多种算子完成Flink文本处理"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"


echo ""
echo "【步骤1】使用命名管道启动 Socket 数据源并自动发送测试数据："

FIFO=/tmp/flink_text_fifo_$$
rm -f "${FIFO}" || true
mkfifo "${FIFO}"

# 启动 nc 监听 9999，将 FIFO 内容发送到客户端
( nc -l 9999 < "${FIFO}" ) &
NC_PID=$!
sleep 1

# 后台提交 Flink 作业
FLINK_RUN_LOG="${FLINK_HOME}/log/run-textproc-$(date +%s).log"
nohup ${FLINK_HOME}/bin/flink run \
    -c myjob.TextProcessingAFilter \
    $JAR_FILE \
    --host master --port 9999 >"${FLINK_RUN_LOG}" 2>&1 &
FLINK_PID=$!

# 写入测试数据到 FIFO 并等待
printf "This is a sample input text with some words\nIt contains several words that have the letter a\nThese words will be counted by the Flink program\n" > "${FIFO}"
sleep 8

# 关闭 nc 并清理
kill ${NC_PID} 2>/dev/null || true
wait ${NC_PID} 2>/dev/null || true
rm -f "${FIFO}"

cat "${FLINK_RUN_LOG}"

echo "预期输出：仅包含字母 'a' 的单词及其计数"
echo "========================================"
