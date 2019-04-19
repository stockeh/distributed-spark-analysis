# distributed-spark-analysis
Using Apache Spark to Analyze the Instacart Dataset

...

## Getting Started

This project makes use of Apache Spark for general-purpose cluster-computing, Hadoop Distributed File System (HDFS) for primary storage, and sbt as a build tool for Scala projects.

It is assumed that HDFS is configured prior to running the application. Apache Spark will need to be downloaded from [Apache's website](https://spark.apache.org/downloads.html) - version 2.4.1 was used for the inital commit.  

Make sure to update and source the local `~/.bashrc` file with the following environment variables: 

```
export SPARK_HOME=${HOME}/spark-2.4.1-bin-hadoop2.7
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:/usr/local/sbt/bin

alias hstartdfs="$HADOOP_HOME/sbin/start-dfs.sh"
alias hstopdfs="$HADOOP_HOME/sbin/stop-dfs.sh"

alias sstartall=$SPARK_HOME/sbin/start-all.sh
alias sstopall=$SPARK_HOME/sbin/stop-all.sh
```
