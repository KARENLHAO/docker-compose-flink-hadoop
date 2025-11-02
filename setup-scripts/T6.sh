#!/bin/bash
###############################################################################
# 实验6：自定义触发器实现窗口数据求和（自动提交版本）
###############################################################################

echo "========================================"
echo "实验6：自定义触发器累计到 N 个元素时计算窗口数据和"
echo "========================================"

JAR_FILE="/workspace/java/target/flink-1.0-SNAPSHOT.jar"
ENTRY_CLASS="myjob.SocketTriggerWindowSum"
WINDOW_SEC=5
TRIGGER_COUNT=5

if [ ! -f "$JAR_FILE" ]; then
    echo "错误: 找不到 JAR 文件 ($JAR_FILE)"
    echo "请先执行: mvn clean package -DskipTests"
    exit 1
fi

echo ""

echo "【参数】窗口 = ${WINDOW_SEC}s, 触发阈值 = ${TRIGGER_COUNT} 条"
echo "【执行】提交 Flink 作业中 ..."
echo ""

# 运行作业（后台 detached 模式）
JOB_OUTPUT=$(${FLINK_HOME}/bin/flink run -d \
    -c ${ENTRY_CLASS} \
    ${JAR_FILE} \
    --window ${WINDOW_SEC} --count ${TRIGGER_COUNT} --min 1 --max 10 --delay 500 2>&1)

echo "${JOB_OUTPUT}"

# 提取 JobID
JOB_ID=$(echo "${JOB_OUTPUT}" | grep -Eo "[0-9a-f]{32}")
if [ -n "${JOB_ID}" ]; then
    echo ""
    echo "✅ Flink 作业已成功提交"
    echo "JobID: ${JOB_ID}"
    echo "----------------------------------------"
    echo "📋 查看运行中的作业:  ${FLINK_HOME}/bin/flink list"
    echo "🧾 查看日志输出:       tail -f ${FLINK_HOME}/log/flink-*-taskexecutor-*.out"
    echo "🛑 停止作业:            ${FLINK_HOME}/bin/flink cancel ${JOB_ID}"
    echo "----------------------------------------"
else
    echo ""
    echo "❌ 提交失败，请检查上方错误输出"
    exit 1
fi

# 等待几秒查看输出
echo ""
echo "等待 Flink 任务输出中..."
sleep 8
LOG_FILE=$(ls -t ${FLINK_HOME}/log/flink-*-taskexecutor-*.out 2>/dev/null | head -n 1)

if [ -n "${LOG_FILE}" ]; then
    echo "----------------------------------------"
    echo "最近任务日志 (节选):"
    tail -n 25 "${LOG_FILE}"
    echo "----------------------------------------"

echo ""
echo "✅ 实验6 自动提交脚本执行完毕"
echo "实验结束后可使用以下命令停止 Flink 集群："

