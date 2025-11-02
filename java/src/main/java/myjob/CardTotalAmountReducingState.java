package myjob;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实验8-4：利用归约状态（ReducingState）统计每张卡的消费总金额（教学版）
 *
 * 核心要点：
 * - ReducingState 允许在状态内部以自定义的 ReduceFunction 累加新的值（这里简单相加金额）。
 * - 与 ValueState 不同，ReducingState 内部维护累加逻辑，使用更语义化的接口。
 *
 * 输入格式：cardNumber,amount,timestamp
 */
public class CardTotalAmountReducingState {

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

        DataStream<String> totalAmounts = transactions
                .keyBy(t -> t.cardNumber)
                .process(new TotalAmountCalculator());

        totalAmounts.print();

        env.execute("Exp8-4-CardTotalAmountReducingState");
    }

    static class TotalAmountCalculator extends KeyedProcessFunction<String, Transaction, String> {
        private transient ReducingState<Double> totalAmountState;

        @Override
        public void open(Configuration parameters) {
            ReducingStateDescriptor<Double> descriptor = new ReducingStateDescriptor<>(
                    "totalAmount",
                    new ReduceFunction<Double>() {
                        @Override
                        public Double reduce(Double value1, Double value2) {
                            return value1 + value2;
                        }
                    },
                    Double.class);
            totalAmountState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
            totalAmountState.add(transaction.amount);
            Double total = totalAmountState.get();

            // 输出当前 key（cardNumber）的累计总额。注意：ReducingState.get() 返回当前累加后的结果
            out.collect(String.format("卡号=%s 总消费金额=%.2f", transaction.cardNumber, total));
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
