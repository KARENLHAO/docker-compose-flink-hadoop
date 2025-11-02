# 使用 Ubuntu 20.04 作为基础镜像
FROM ubuntu:20.04

# 设置环境变量避免交互式安装
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Asia/Shanghai

# 安装基础软件和依赖（包含 SSH、Maven、Java 等）
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    maven \
    openssh-server \
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
# 1️⃣ 创建统一的 hadoop 用户
############################
RUN useradd -m -s /bin/bash hadoop && \
    echo "hadoop:hadoop" | chpasswd && \
    echo "hadoop ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

############################
# 2️⃣ 配置 SSH 基础设置
############################
RUN mkdir -p /var/run/sshd && \
    sed -i 's/#Port 22/Port 22/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    echo "UseDNS no" >> /etc/ssh/sshd_config

############################
# 3️⃣ 切换到 hadoop 用户，生成免密 SSH 密钥
############################
USER hadoop
WORKDIR /home/hadoop

RUN mkdir -p ~/.ssh && \
    ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 700 ~/.ssh && \
    chmod 600 ~/.ssh/authorized_keys

############################
# 4️⃣ 下载并安装 Hadoop & Flink
############################
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

# Flink Hadoop 连接器
USER root
RUN cd /tmp && \
    wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar && \
    mv flink-shaded-hadoop-2-uber-2.8.3-10.0.jar /home/hadoop/flink/lib/ && \
    chown hadoop:hadoop /home/hadoop/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

############################
# 5️⃣ 设置环境变量
############################
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/home/hadoop/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV YARN_HOME=${HADOOP_HOME}
ENV FLINK_HOME=/home/hadoop/flink
ENV MAVEN_HOME=/usr/share/maven
ENV PATH=${JAVA_HOME}/bin:${MAVEN_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${FLINK_HOME}/bin:$PATH

# 把环境变量写进 hadoop 用户 bashrc
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64" >> /home/hadoop/.bashrc && \
    echo "export HADOOP_HOME=/home/hadoop/hadoop" >> /home/hadoop/.bashrc && \
    echo "export HADOOP_CONF_DIR=\${HADOOP_HOME}/etc/hadoop" >> /home/hadoop/.bashrc && \
    echo "export FLINK_HOME=/home/hadoop/flink" >> /home/hadoop/.bashrc && \
    echo "export PATH=\${JAVA_HOME}/bin:\${HADOOP_HOME}/bin:\${HADOOP_HOME}/sbin:\${FLINK_HOME}/bin:\${PATH}" >> /home/hadoop/.bashrc && \
    chown hadoop:hadoop /home/hadoop/.bashrc

############################
# 6️⃣ 预建 Hadoop 数据目录
############################
RUN mkdir -p /home/hadoop/data/namenode /home/hadoop/data/datanode /home/hadoop/data/tmp && \
    chown -R hadoop:hadoop /home/hadoop/data

############################
# 7️⃣ 切回 root，暴露端口
############################
USER root

# Hadoop Ports
EXPOSE 9000 9870 8088 19888 50070 50075 50010 50020 50090 8020
# Flink Ports
EXPOSE 8081 6123 6124 6125
# SSH
EXPOSE 22

WORKDIR /workspace

############################
# 8️⃣ 启动命令
#    - 启动 sshd
#    - 保持前台运行
############################
CMD ["/usr/sbin/sshd", "-D"]
