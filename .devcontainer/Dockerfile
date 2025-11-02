# 使用 Ubuntu 20.04 作为基础镜像
FROM ubuntu:20.04

# 设置环境变量避免交互式安装
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# 安装基础软件和依赖（包含 Maven、OpenSSH 服务端等）
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    maven \
    openssh-server \
    openssh-client \
    rsync \
    vim \
    wget \
    curl \
    sudo \
    net-tools \
    iputils-ping \
    netcat \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

############################
# 1. 创建统一的 hadoop 用户
############################
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

############################
# 2. 配置 SSH 基础
############################
# - 开启 sshd 跑所需目录
# - 允许公钥认证
# - 容许我们用密码登录（第一次调试时有用）
#   但真正日常用的是公钥免密
#
# 注意：我们并不依赖 root 去互相 ssh，后面我们将用 hadoop 用户。
############################
RUN mkdir -p /var/run/sshd && \
    sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^PasswordAuthentication no/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

############################
# 3. 为 hadoop 用户准备同一把 SSH 密钥，并写入 authorized_keys
#
# 关键点：我们在镜像构建阶段（root 身份）直接生成这对密钥，
# 然后把同一对密钥放进 /home/hadoop/.ssh，这样所有基于这个镜像启动的
# 容器都会有**同一把**私钥 & 公钥，自然就互信了。
############################
RUN mkdir -p /home/hadoop/.ssh && \
    ssh-keygen -t rsa -b 4096 -N "" -f /home/hadoop/.ssh/id_rsa && \
    cat /home/hadoop/.ssh/id_rsa.pub >> /home/hadoop/.ssh/authorized_keys && \
    chmod 700 /home/hadoop/.ssh && \
    chmod 600 /home/hadoop/.ssh/authorized_keys && \
    chown -R hadoop:hadoop /home/hadoop/.ssh

############################
# 4. 安装 Hadoop 3.3.5 和 Flink 1.17.0
############################
USER hadoop
WORKDIR /home/hadoop

# Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz && \
    tar -zxf hadoop-3.3.5.tar.gz && \
    rm hadoop-3.3.5.tar.gz && \
    mv hadoop-3.3.5 hadoop

# Flink
RUN wget https://archive.apache.org/dist/flink/flink-1.17.0/flink-1.17.0-bin-scala_2.12.tgz && \
    tar -zxf flink-1.17.0-bin-scala_2.12.tgz && \
    rm flink-1.17.0-bin-scala_2.12.tgz && \
    mv flink-1.17.0 flink

# Hadoop FS connector for Flink (HDFS support JAR)
USER root
RUN cd /tmp && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar && \
    mv /tmp/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar /home/hadoop/flink/lib/ && \
    chown hadoop:hadoop /home/hadoop/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

############################
# 5. 环境变量
############################
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV JRE_HOME=${JAVA_HOME}/jre
ENV CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib

ENV HADOOP_HOME=/home/hadoop/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV YARN_HOME=${HADOOP_HOME}

ENV FLINK_HOME=/home/hadoop/flink
ENV MAVEN_HOME=/usr/share/maven

ENV PATH=${MAVEN_HOME}/bin:${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${FLINK_HOME}/bin:${PATH}

############################
# 6. 把环境变量也写进 hadoop 用户的 bashrc
############################
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /home/hadoop/.bashrc && \
    echo "export HADOOP_HOME=/home/hadoop/hadoop" >> /home/hadoop/.bashrc && \
    echo "export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop" >> /home/hadoop/.bashrc && \
    echo "export FLINK_HOME=/home/hadoop/flink" >> /home/hadoop/.bashrc && \
    echo "export MAVEN_HOME=/usr/share/maven" >> /home/hadoop/.bashrc && \
    echo "export PATH=\${MAVEN_HOME}/bin:\${JAVA_HOME}/bin:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin:\${FLINK_HOME}/bin:\${PATH}" >> /home/hadoop/.bashrc && \
    chown hadoop:hadoop /home/hadoop/.bashrc

############################
# 7. 为 HDFS / YARN 预建数据目录（属主要是 hadoop）
############################
RUN mkdir -p /home/hadoop/data/namenode && \
    mkdir -p /home/hadoop/data/datanode && \
    mkdir -p /home/hadoop/data/tmp && \
    chown -R hadoop:hadoop /home/hadoop/data

############################
# 8. 切回 root 做最终暴露和入口
############################
USER root

# 暴露端口
# Hadoop NN / RM / HistoryServer / etc
EXPOSE 9000 9870 8088 19888 50070 50075 50010 50020 50090 8020
# Flink WebUI / RPC / Data ports
EXPOSE 8081 6123 6124 6125
# SSH
EXPOSE 22

# 工作目录（之后你 docker exec 进来可以 cd /workspace 做操作）
WORKDIR /workspace

############################
# 9. 启动时要确保 sshd 在跑
#    我们用一个简单的 CMD: 先启动 sshd，然后给你个 bash。
#    在 docker-compose 里多个容器都会跑 sshd，这样互相才能 ssh/scp。
############################
CMD service ssh start && bash
