package myjob;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * 实验8-3：利用映射状态（MapState）统计每张卡每种交易金额出现的次数（教学版）
 *
 * 核心要点：
 * - 每张卡使用一个 MapState<Double, Integer> 来记录“金额 -> 出现次数”。
 * - MapState 适用于以键值对形式存储任意数量的子条目（与 ValueState 不同）。
 *
 * 输入数据格式（每行）：
 * cardNumber,amount,timestamp
 * 例如："622202xxxxxxxxxx,100.50,1627890123"
 *
 * 教学提示：
 * - 讨论 MapState 在状态大小和序列化方面的影响；如果金额种类很多，MapState 的存储开销会增加。
 * - 演示如何遍历 MapState.entries() 来生成可打印的统计信息。
 */
public class CardAmountCountMapState {

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

        DataStream<String> amountCounts = transactions
                .keyBy(t -> t.cardNumber)
                .process(new AmountCounter());

        amountCounts.print();

        env.execute("Exp8-3-CardAmountCountMapState");
    }

    static class AmountCounter extends KeyedProcessFunction<String, Transaction, String> {
        private transient MapState<Double, Integer> amountCountState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<Double, Integer> descriptor = new MapStateDescriptor<>("amountCount", Double.class,
                    Integer.class);
            amountCountState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context ctx, Collector<String> out) throws Exception {
            Integer count = amountCountState.get(transaction.amount);
            if (count == null) {
                count = 0;
            }
            count++;
            amountCountState.put(transaction.amount, count);

            Map<Double, Integer> allCounts = new HashMap<>();
            for (Map.Entry<Double, Integer> entry : amountCountState.entries()) {
                allCounts.put(entry.getKey(), entry.getValue());
            }

            // 输出：KeyedProcessFunction 运行在 cardNumber 的分区上，因此 transaction.cardNumber 是当前 key
            // 将 MapState 的内容拷贝到普通 Map 以便打印。这步不用在生产中频繁做，主要为了课堂展示。
            out.collect(String.format("卡号=%s 金额统计: %s", transaction.cardNumber, allCounts));
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
