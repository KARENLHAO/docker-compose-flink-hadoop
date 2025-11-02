package myjob;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * 实验6：自定义触发器实现窗口数据求和
 *
 * 需求回顾：
 *  - 数据流中的每个元素代表一个整数；
 *  - 在固定时间窗口内累积数字；
 *  - 自定义触发器：当累计到一定数量的元素时，立即触发计算并输出窗口内元素之和；
 *  - 输出格式类似：
 *      Input Data:3> 10
 *      ...
 *      Sum of window:2> 27
 *
 * 思路：
 *  1. 用 RichParallelSourceFunction 持续随机生成整数，模拟“实时输入”；
 *  2. 打印每条输入（Input Data:...）便于观察；
 *  3. 使用 windowAll(...) + 自定义 Trigger；
 *  4. Trigger 满足阈值时 FIRE_AND_PURGE，调用 ProcessAllWindowFunction 计算窗口和并输出；
 *  5. PURGE 会清空窗口里的元素，为下一轮做准备。
 */
public class SocketTriggerWindowSum {

    public static void main(String[] args) throws Exception {

        // 参数：窗口长度、触发阈值、随机最小值/最大值等可以通过 args 调整
        int windowSec = 5;      // 窗口时长(处理时间窗口)
        int fireCount = 5;      // 每多少个元素触发一次求和
        int minValue  = 1;      // 生成整数的下界
        int maxValue  = 10;     // 生成整数的上界
        long delayMs  = 500L;   // 每条数据间隔（毫秒）

        for (int i = 0; i + 1 < args.length; i++) {
            if ("--window".equals(args[i])) {
                windowSec = Integer.parseInt(args[i + 1]);
            } else if ("--count".equals(args[i])) {
                fireCount = Integer.parseInt(args[i + 1]);
            } else if ("--min".equals(args[i])) {
                minValue = Integer.parseInt(args[i + 1]);
            } else if ("--max".equals(args[i])) {
                maxValue = Integer.parseInt(args[i + 1]);
            } else if ("--delay".equals(args[i])) {
                delayMs = Long.parseLong(args[i + 1]);
            }
        }

        // 1. 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 为了让输出行为和课堂示例顺序一致，设为1

        // 2. 自定义Source：生成随机整数
        DataStream<Integer> numbers = env.addSource(
                new RandomIntSource(minValue, maxValue, delayMs)
        ).name("random-int-source");

        // 打印“输入流”，模仿示例里的 Input Data:x> v 行
        DataStream<String> incoming = numbers
                .map(v -> "Input Data> " + v);
        incoming.print();

        // 3. 定义窗口 + 自定义触发器
        DataStream<String> summed = numbers
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(windowSec)))
                .trigger(new ElementCountTrigger(fireCount))
                .process(new ProcessAllWindowFunction<Integer, String, TimeWindow>() {
                    @Override
                    public void process(Context ctx,
                                        Iterable<Integer> elements,
                                        Collector<String> out) {

                        int sum = 0;
                        int count = 0;
                        for (Integer v : elements) {
                            sum += v;
                            count++;
                        }
                        out.collect("Sum of window> " + sum + " (count=" + count + ")");
                    }
                });

        // 输出结果 "Sum of window> ..."
        summed.print();

        // 4. 执行任务
        env.execute("T6");
    }

    /**
     * 随机整数源：
     * - 周期性地产生[minValue, maxValue]之间的随机整数
     * - delayMs 决定发送间隔
     * - 不主动结束，除非 cancel() 被调用（和实验描述一致：流是持续到一定时间）
     */
    public static class RandomIntSource extends RichParallelSourceFunction<Integer> {
        private final int min;
        private final int max;
        private final long delayMs;
        private volatile boolean running = true;
        private transient Random rand;

        public RandomIntSource(int min, int max, long delayMs) {
            this.min = min;
            this.max = max;
            this.delayMs = delayMs;
        }

        @Override
        public void open(Configuration parameters) {
            this.rand = new Random();
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                int value = rand.nextInt(max - min + 1) + min;
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(value);
                }
                Thread.sleep(delayMs);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 自定义触发器：
     * - 用一个 ValueState<Long> 记录“当前窗口已经来了多少个元素”
     * - 每来一个元素：
     *      +1
     *      如果达到阈值 threshold -> FIRE_AND_PURGE
     * - FIRE_AND_PURGE:
     *      FIRE  = 调用窗口函数输出结果
     *      PURGE = 清空窗口里的元素，为下一次统计做准备
     *
     * 注意：我们用的是 windowAll(...)，所以这里是全局窗口，不按 key 分组。
     * 如果是按 key 的窗口，可以用 TriggerContext.getPartitionedState 做分key计数。
     */
    public static class ElementCountTrigger extends Trigger<Integer, TimeWindow> {
        private final int threshold;

        public ElementCountTrigger(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public TriggerResult onElement(Integer element,
                                       long timestamp,
                                       TimeWindow window,
                                       TriggerContext ctx) throws Exception {

            // 用 ValueState 记录计数
            ValueStateDescriptor<Long> desc =
                    new ValueStateDescriptor<>("element-count", Long.class);
            ValueState<Long> countState = ctx.getPartitionedState(desc);

            Long current = countState.value();
            if (current == null) {
                current = 0L;
            }

            current += 1L;

            // 达到触发阈值 => 触发一次并清空
            if (current >= threshold) {
                countState.clear();
                return TriggerResult.FIRE_AND_PURGE;
            } else {
                countState.update(current);
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time,
                                              TimeWindow window,
                                              TriggerContext ctx) {
            // 我们不基于时间触发
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time,
                                         TimeWindow window,
                                         TriggerContext ctx) {
            // 我们不基于事件时间触发
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window,
                          TriggerContext ctx) throws Exception {
            ValueStateDescriptor<Long> desc =
                    new ValueStateDescriptor<>("element-count", Long.class);
            ValueState<Long> countState = ctx.getPartitionedState(desc);
            countState.clear();
        }
    }
}
