package myjob;

/**
 * 公共交易对象与解析工具（教学辅助类）
 *
 * 说明：
 * - 为多个示例程序提供一个统一的 Transaction POJO 和简单的解析函数。
 * - 注意：parse 方法较为宽松，仅用于示例/实验环境；线上应增加校验与异常处理。
 */
public class CardCommon {
    /**
     * 简单的交易 POJO，用于示例中传递卡号、金额与时间戳。
     * 字段均为 public，便于 Flink 的 POJO 反射/序列化（教学示例）。
     */
    public static class Transaction {
        public String cardNumber;
        public double amount;
        public long timestamp; // epoch seconds or ms，可根据示例决定单位

        public Transaction() {
        }

        public Transaction(String cardNumber, double amount, long timestamp) {
            this.cardNumber = cardNumber;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return cardNumber + "," + amount + "," + timestamp;
        }
    }

    /**
     * 将一行文本解析为 Transaction 对象。
     * 输入期望为: cardNumber,amount,timestamp
     * 返回 null 表示解析失败（上层应过滤掉 null）。
     */
    public static Transaction parse(String line) {
        String[] p = line.trim().split(",");
        if (p.length < 3)
            return null; // 简单保护
        return new Transaction(p[0].trim(), Double.parseDouble(p[1].trim()), Long.parseLong(p[2].trim()));
    }
}
