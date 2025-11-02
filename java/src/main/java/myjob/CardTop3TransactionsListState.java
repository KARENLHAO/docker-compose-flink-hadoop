package myjob;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 实验8-2：利用列表状态（ListState）找到每张卡交易金额前三的交易（教学版）
 *
 * 说明：
 * - ListState 用于保存一个 key 下的多条元素，这里我们将每笔交易保存到 ListState，然后在每次到达新交易时排序并选出前 3。
 * - 教学重点是演示 ListState 的基本用法和在内存受限时的潜在问题（全量保存并排序会消耗内存）。
 *
 * 输入格式：cardNumber,amount,timestamp
 */
public class CardTop3TransactionsListState {

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

        DataStream<String> top3 = transactions
                .keyBy(t -> t.cardNumber)
                .process(new Top3Calculator());

        top3.print();

        env.execute("Exp8-2-CardTop3TransactionsListState");
    }

    static class Top3Calculator extends KeyedProcessFunction<String, Transaction, String> {
        private transient ListState<Transaction> transactionsState;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<Transaction> descriptor = new ListStateDescriptor<>("transactions", Transaction.class);
            transactionsState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
            transactionsState.add(transaction);

            List<Transaction> allTransactions = new ArrayList<>();
            for (Transaction t : transactionsState.get()) {
                allTransactions.add(t);
            }

            allTransactions.sort(Comparator.comparingDouble((Transaction t) -> t.amount).reversed());

            List<Transaction> top3List = allTransactions.subList(0, Math.min(3, allTransactions.size()));

            // 输出前三交易的金额（简单展示），实际应用中可以仅保存和维护固定大小的 topK 以降低内存
            out.collect(String.format("卡号=%s 前3交易: %s", transaction.cardNumber, top3List));
        }
    }

    static class Transaction {
        String cardNumber;
        Double amount;
        Long timeStamp;

        public Transaction() {
        }

        public Transaction(String cardNumber, Double amount, Long timeStamp) {
            this.cardNumber = cardNumber;
            this.amount = amount;
            this.timeStamp = timeStamp;
        }

        @Override
        public String toString() {
            return String.format("%.2f", amount);
        }
    }
}
