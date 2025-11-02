# 基于 **Docker** 的分布式实验环境，集成 **Hadoop 3.3.5** 与 **Flink 1.17.0**，用于分布式计算与流式处理实验。

## ⚙️ 一、环境配置

| 组件         | 版本                    | 说明           |
| ---------- | --------------------- | ------------ |
| **ubuntu** | 20.04                | 集群基础操作系统环境 |
| **Hadoop** | 3.3.5                 | 分布式文件系统与计算框架 |
| **Flink**  | 1.17.0                | 流式/批处理计算引擎   |
| **Java**   | OpenJDK 8             | 运行时环境        |
| **节点配置**   | 1 × Master，2 × Worker | 三节点集群结构      |

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


