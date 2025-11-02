package myjob;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

/**
 * 实验3：使用窗口计算函数 AggregateFunction 计算给定窗口时间内的订单销售额（示例版）
 *
 * 说明：
 * - 目标：按产品（product）统计每个时间窗口内的销售总额（subtotal 的累加）。
 * - 技术点：使用 AggregateFunction 做增量聚合，避免把整个窗口数据保存在内存中，适合数值累加类场景。
 * - 输入格式：orderId,productName,quantity
 * - 价格来源：为了示例方便，使用内置 priceTable；真实场景中可用外部配置、数据库或广播状态。
 *
 * 注：本文件在关键位置加入中文注释，便于理解每一行代码的意图和实现细节。
 */
public class OrderSalesAggregate {

    public static void main(String[] args) throws Exception {
        // ...existing code...

        String host = "localhost";
        int port = 9999;
        int windowSec = 10;

        // 内置价格表（示例用）
        // 说明：这里简化处理，直接在代码里给出商品到单价的映射。
        // 注意：真实系统中，价格可能来自产品服务或库存系统，需要做容错和更新策略。
        Map<String, Double> priceTable = new HashMap<>();
        priceTable.put("towel", 10.0);
        priceTable.put("toothbrush", 5.0);
        priceTable.put("instantnoodles", 3.0);

        // 命令行参数解析（可扩展）
        // 说明：通过 --host/--port/--window 传参以便调试或演示。
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
            if ("--window".equals(args[i]))
                windowSec = Integer.parseInt(args[i + 1]);
        }

        // 获得 Flink 流执行环境，设置并行度为 1 以便观察输出顺序
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用 socket 数据源，便于通过 nc/netcat 或 telnet 发送文本数据进行演示
        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        // 将原始文本行转换为 (product, subtotal) 形式的流
        DataStream<Tuple2<String, Double>> salesPerProduct = lines
                // 过滤空行，说明：输入往往不规整，先做健壮性检查
                .filter(s -> s != null && !s.trim().isEmpty())
                // 按逗号分割，期望得到三个字段：orderId,productName,quantity
                .map(s -> s.trim().split(","))
                // 过滤掉解析失败的行，说明：和流数据质量相关的常见处理
                .filter(arr -> arr.length == 3)
                // 将解析好的字段转换成 (product, qty * price) 的二元组
                .map(arr -> {
                    String product = arr[1].trim();
                    int qty = Integer.parseInt(arr[2].trim());
                    // 若 priceTable 中没有对应商品，则使用默认价格 1.0（示例简化）
                    double price = priceTable.getOrDefault(product, 1.0);
                    // 返回 (product, subtotal)
                    return Tuple2.of(product, qty * price);
                })
                // 显式声明返回类型，避免泛型擦除导致的类型推断问题
                .returns(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Double>>() {
                });

        // 按 product 分组、使用滚动时间窗口，并在窗口内使用 AggregateFunction 做增量聚合
        DataStream<Tuple2<String, Double>> windowSales = salesPerProduct
                // keyBy：按 product 分组，f0 为商品名称
                .keyBy(t -> t.f0)
                // 使用处理时间的滚动窗口，窗口长度由 windowSec 决定
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSec)))
                // 使用 AggregateFunction 做增量聚合（关键点）
                .aggregate(new AggregateFunction<Tuple2<String, Double>, Double, Tuple2<String, Double>>() {
                    // 说明：这里用 Double 作为累加器（accumulator），只保存当前累计金额
                    // currentKey 用来记录当前分组的 key（product），以便在 getResult 中与累加值一起返回
                    private transient String currentKey;

                    @Override
                    public Double createAccumulator() {
                        // createAccumulator 在每个窗口（每个分组）开始时调用一次，用于创建初始累加器
                        // 说明：对于数值累加，只需返回 0.0 即可
                        return 0.0;
                    }

                    @Override
                    public Double add(Tuple2<String, Double> value, Double acc) {
                        // add 在窗口内每来一条记录时调用：把本条记录的 subtotal 加到累加器上
                        // 提示：value.f1 是 map 时计算的 qty * price（小计）
                        // 同时记录当前 key（product），getResult 需要它来构造输出的二元组
                        currentKey = value.f0;
                        return acc + value.f1;
                    }

                    @Override
                    public Tuple2<String, Double> getResult(Double acc) {
                        // getResult 在窗口触发时调用：把累加器的值包装成输出类型
                        // 说明：返回 (product, totalSales)，注意 currentKey 已在 add 中被设置
                        return Tuple2.of(currentKey, acc);
                    }

                    @Override
                    public Double merge(Double a, Double b) {
                        // merge 用于会合两个累加器（在会发生合并的窗口策略或会话窗口中可能用到）
                        // 提示：对于简单加和，直接相加即可
                        return a + b;
                    }
                });

        // 将窗口统计结果打印到控制台，便于观察
        windowSales.print();
        env.execute("T3");
    }
}
