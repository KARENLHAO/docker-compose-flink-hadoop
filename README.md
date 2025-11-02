# 本项目提供一个基于 Docker 的分布式实验环境，集成 Hadoop 与 Flink，支持 分布式文件系统 与 流式计算 实验。(⚠️本仓库及其代码仅为作者本人的学习与实验，如有报错请自行解决)

本项目内置了一些练习的Job,你可以阅读[pdf](<Flink DataStream API编程实践.pdf>)查看具体的练习题目
```bash
java/
└── src/
    └── main/
        └── java/
            └── myjob/
                ├── BatchWordCount.java               # 批处理词频统计示例，演示 DataSet API 的使用
                ├── StreamWordCount.java              # 实时流式词频统计，基于 DataStream API
                ├── SocketTriggerWindowSum.java       # 自定义触发器实现窗口数据求和
                ├── SensorMaxTemperatureReduce.java   # 使用窗口计算函数ReduceFunction 统计传感器温度最大值
                ├── TextProcessingAFilter.java        # 使用多种算子完成Flink 文本处理实验
                ├── VehicleCategoryCountProcessWindow.java  # 使用窗口计算函数ProcessWindowFunction 完成车辆种类数量统计
                ├── WaterSensorMaxLevelAggAndProcess.java    # 基于增量聚合和全窗口函数的水传感器最大水位值分析
                └── OrderSalesAggregate.java          # 使用窗口计算函数AggregateFunction计算给定窗口时间内的订单销售额
```
如果你有自己题目，你可以通过`mvn clean package -DskipTests`来打包成jar，打包后会在`java\target\flink-1.0-SNAPSHOT.jar`

## ⚙️ 一、环境配置

| 组件         | 版本                    | 说明           |
| ---------- | --------------------- | ------------ |
| **ubuntu** | 20.04                | 集群基础操作系统环境 |
| **Hadoop** | 3.3.5                 | 分布式文件系统与计算框架 |
| **Flink**  | 1.17.0                | 流式/批处理计算引擎   |
| **Java**   | OpenJDK 8             | 运行时环境        |
| **节点配置**   | 1 × Master，2 × Worker | 三节点集群结构      |

### 快速开始

#### 1. 启动 DevContainer

- 在 VS Code 中打开此项目
- 按 `F1` 或 `Ctrl+Shift+P` 打开命令面板
- 选择 `Dev Containers: Reopen in Container`
- 等待容器构建和启动（首次启动可能需要较长时间）

#### 2. 初始化集群
```bash
sudo chmod +x setup-scripts/*.sh
./setup-scripts/init-cluster.sh
```
---

## 🧪 二、实验内容

### 🧩 实验一：Flink Standalone 模式

#### 1. 启动 Hadoop 集群

在 master 节点执行以下命令（如已启动可跳过）：

```bash
./setup-scripts/start-hadoop.sh
```

**访问 Web UI：**
| 模块                       | 默认端口 | 访问地址                                           |
| ------------------------ | ---- | ---------------------------------------------- |
| **HDFS NameNode**        | 9870 | [http://localhost:9870](http://localhost:9870) |
| **YARN ResourceManager** | 8088 | [http://localhost:8088](http://localhost:8088) |
---

#### 2. 启动 Flink Standalone 集群

```bash
./setup-scripts/start-flink-standalone.sh
```

**访问 Flink Web UI：**

| 模块                       | 默认端口 | 访问地址                                           |
| ------------------------ | ---- | ---------------------------------------------- |
| **Flink Dashboard**      | 8081 | [http://localhost:8081](http://localhost:8081) |

---

#### 3. 运行 WordCount 示例


```bash
./setup-scripts/run-wordcount-standalone.sh
```

**结果：**

* 在 HDFS 路径 `/flink-test/wordcount-result-standalone` 下生成词频统计结果文件。

**查看：**
```bash
hdfs dfs -cat /flink-test/wordcount-result-standalone
```
---


#### 4. 停止集群

按顺序关闭 Flink 与 Hadoop：

```bash
# 停止 Flink 集群
./setup-scripts/stop-flink-standalone.sh

# 停止 Hadoop 集群
./setup-scripts/stop-hadoop.sh
```

---

### 🧩 实验二：Flink on YARN 模式

#### 1. 启动 Hadoop 集群

```bash
./setup-scripts/start-hadoop.sh
```

---

#### 2. 运行 WordCount 示例

在 master 节点执行以下命令：

```bash
./setup-scripts/run-wordcount-yarn.sh
```

---

## 🧭 三、访问界面总览

| 模块                       | 默认端口 | 访问地址                                           |
| ------------------------ | ---- | ---------------------------------------------- |
| **HDFS NameNode**        | 9870 | [http://localhost:9870](http://localhost:9870) |
| **YARN ResourceManager** | 8088 | [http://localhost:8088](http://localhost:8088) |
| **Flink Dashboard**      | 8081 | [http://localhost:8081](http://localhost:8081) |

---


