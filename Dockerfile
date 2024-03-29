FROM openjdk:11.0.11-jre-slim-buster as builder

ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=2.7
ENV HADOOP_FULL_VERSION=2.7.0
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/spark
ENV PYTHONHASHSEED=1
ENV PATH $PATH:$HADOOP_HOME/bin
ENV PATH $PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON python3

RUN apt-get update && apt-get install -y curl vim wget software-properties-common ssh net-tools ca-certificates python3 python3-pip

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Spark
RUN wget --no-verbose -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
&& mkdir -p /opt/spark \
&& tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
&& rm apache-spark.tgz

# Hadoop
RUN wget --no-verbose -O apache-hadoop.tgz "http://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_FULL_VERSION}/hadoop-${HADOOP_FULL_VERSION}.tar.gz" \
&& tar -xf apache-hadoop.tgz -C /opt/spark --strip-components=1 \
&& rm apache-hadoop.tgz

FROM builder as apache-spark
WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
SPARK_MASTER_WEBUI_PORT=8080 \
SPARK_LOG_DIR=/opt/spark/logs \
SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
SPARK_WORKER_WEBUI_PORT=8080 \
SPARK_WORKER_PORT=7000 \
SPARK_MASTER="spark://spark-master:7077" \
SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000

RUN mkdir -p $SPARK_LOG_DIR && \
touch $SPARK_MASTER_LOG && \
touch $SPARK_WORKER_LOG && \
ln -sf /dev/stdout $SPARK_MASTER_LOG && \
ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY start-spark.sh /

ADD requirements.txt /opt/spark


RUN python3 -m pip install --upgrade pip
RUN pip install -r requirements.txt

CMD ["/bin/bash", "/start-spark.sh"]
