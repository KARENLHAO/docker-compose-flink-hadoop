package myjob;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 实验8-1：利用值状态（ValueState）检测信用卡欺诈（教学版）
 *
 * 业务规则：当同一张卡连续两笔交易金额都超过 5000 时，认为存在欺诈并发出警报。
 *
 * 核心要点：
 * - ValueState 用来保存上一次交易金额（简单场景）
 * - KeyedProcessFunction 基于 key（cardNumber）运行，ValueState 是每个 key 的私有状态
 *
 * 输入格式：cardNumber,amount,timestamp
 */
public class CardFraudDetectionValueState {

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

        DataStream<String> alerts = transactions
                .keyBy(t -> t.cardNumber)
                .process(new FraudDetector());

        alerts.print();

        env.execute("Exp8-1-CardFraudDetectionValueState");
    }

    static class FraudDetector extends KeyedProcessFunction<String, Transaction, String> {
        private transient ValueState<Double> lastAmountState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("lastAmount", Double.class);
            lastAmountState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
            Double lastAmount = lastAmountState.value();

            // 检查条件：上一次交易存在且都超过阈值
            if (lastAmount != null && lastAmount > 5000 && transaction.amount > 5000) {
                // 触发报警：这里简单输出字符串，课堂上可扩展为写入外部报警系统
                out.collect(String.format("⚠️ 欺诈报警：卡号=%s 连续大额交易 上次=%.2f 本次=%.2f",
                        transaction.cardNumber, lastAmount, transaction.amount));
            }

            // 更新状态为当前交易金额（无论是否报警都要更新）
            lastAmountState.update(transaction.amount);
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

        @Override
        public String toString() {
            return String.format("(%s,%.2f,%d)", cardNumber, amount, timeStamp);
        }
    }
}
