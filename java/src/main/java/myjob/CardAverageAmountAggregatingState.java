package myjob;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实验8-5：利用聚合状态（AggregatingState）统计每张卡的消费平均金额（教学版）
 *
 * 核心要点：
 * - AggregatingState 允许以增量方式维护更复杂的聚合（这里我们用 (sum, count) -> avg）。
 * - 与 ReduceState/AccumulatingState 比较：AggregatingState 可以为输出类型做转换（accumulator
 * -> result）。
 *
 * 输入数据格式：cardNumber,amount,timestamp
 * 例如："622202...,23.5,1627890123"
 *
 * 教学提示：
 * - 演示自定义 AggregateFunction 的四个方法：createAccumulator, add, getResult, merge
 * - 注意累加器类型和输出类型不同（这里累加器是 Tuple2<sum, count>, 输出是 Double）
 */
public class CardAverageAmountAggregatingState {

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 9999;

        for (int i = 0; i + 1 < args.length; i++) {
            if ("--host".equals(args[i]))
                host = args[i + 1];
            if ("--port".equals(args[i]))
                port = Integer.parseInt(args[i + 1]);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> lines = env.socketTextStream(host, port, "\n");

        DataStream<Transaction> transactions = lines
                .filter(s -> s != null && !s.trim().isEmpty())
                .map(s -> {
                    String[] parts = s.trim().split(",");
                    return new Transaction(parts[0].trim(), Double.parseDouble(parts[1].trim()),
                            Long.parseLong(parts[2].trim()));
                });

        DataStream<String> avgAmounts = transactions
                .keyBy(t -> t.cardNumber)
                .process(new AverageAmountCalculator());

        avgAmounts.print();

        env.execute("Exp8-5-CardAverageAmountAggregatingState");
    }

    static class AverageAmountCalculator extends KeyedProcessFunction<String, Transaction, String> {
        private transient AggregatingState<Double, Double> avgAmountState;

        @Override
        public void open(Configuration parameters) {
            AggregatingStateDescriptor<Double, Tuple2<Double, Long>, Double> descriptor = new AggregatingStateDescriptor<>(
                    "avgAmount",
                    new AggregateFunction<Double, Tuple2<Double, Long>, Double>() {
                        @Override
                        public Tuple2<Double, Long> createAccumulator() {
                            // 初始累加器：sum=0.0, count=0
                            return Tuple2.of(0.0, 0L);
                        }

                        @Override
                        public Tuple2<Double, Long> add(Double value, Tuple2<Double, Long> accumulator) {
                            // 收到一个新的金额，更新 sum 与 count
                            return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                        }

                        @Override
                        public Double getResult(Tuple2<Double, Long> accumulator) {
                            // 将累加器转换为最终结果：平均值（处理除零）
                            return accumulator.f1 == 0 ? 0.0 : accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
                            // 当 Flink 需要合并多个累加器（例如窗口合并）时，合并 sum 和 count
                            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                        }
                    },
                    org.apache.flink.api.common.typeinfo.Types.TUPLE(org.apache.flink.api.common.typeinfo.Types.DOUBLE,
                            org.apache.flink.api.common.typeinfo.Types.LONG));
            avgAmountState = getRuntimeContext().getAggregatingState(descriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
            avgAmountState.add(transaction.amount);
            Double avg = avgAmountState.get();

            out.collect(String.format("卡号=%s 平均消费金额=%.2f", transaction.cardNumber, avg));
        }
    }

    static class Transaction {
        String cardNumber;
        Double amount;
        Long timeStamp;

        public Transaction(String cardNumber, Double amount, Long timeStamp) {
            this.cardNumber = cardNumber;
            this.amount = amount;
            this.timeStamp = timeStamp;
        }
    }
}
