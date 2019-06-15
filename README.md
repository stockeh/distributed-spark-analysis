# distributed-spark-analysis
Using Apache Spark to Analyze the Instacart Dataset

### Abstract
Purchasing nutritional food products can be overwhelming when faced with the enormous amount of items available in grocery stores today. Through the rise of ecommerce and data collection, we can use data analytic methods to help consumers find healthy foods and make the best decisions for their diet. By leveraging data available from the online grocery delivery service, Instacart, and the United States Department of Agriculture, we were able to analyze the nutritional makeup of food products and recommend items to users based on their interests using machine learning techniques. Dietary and nutritional strategies of users were analyzed, recommendations were created with collaborative filtering, and food was broken down into its primary nutrients and explored via principal component analysis. Our findings indicate that Instacart users make relatively healthy choices, that matrix factorization is an effective approach to create user recommendations, and that categorizing foods by their nutrients opens the door for more research.

## Getting Started

This project makes use of Apache Spark for general-purpose cluster-computing, Hadoop Distributed File System (HDFS) for primary storage, and sbt as a build tool for Scala projects.

It is assumed that HDFS is configured prior to running the application. Apache Spark will need to be downloaded from [Apache's website](https://spark.apache.org/downloads.html) - version 2.4.1 was used for the inital commit.  

To use the given `run.sh` script make sure to update and source the local `~/.bashrc` file with the following environment variables: 

```
export SPARK_HOME=${HOME}/spark-2.4.1-bin-hadoop2.7
export HADOOP_HOME=/usr/local/hadoop
export PATH=$PATH:/usr/local/sbt/bin

alias hstartdfs="$HADOOP_HOME/sbin/start-dfs.sh"
alias hstopdfs="$HADOOP_HOME/sbin/stop-dfs.sh"

alias sstartall=$SPARK_HOME/sbin/start-all.sh
alias sstopall=$SPARK_HOME/sbin/stop-all.sh
```

## Data Access

The [Instacart dataset](https://www.instacart.com/datasets/grocery-shopping-2017) describes 3 Million Instacart orders, and can be described [here](https://gist.github.com/jeremystan/c3b39d947d9b88b3ccff3147dbcf6c6b).

Data for the [USDA Food Composition Databases](https://ndb.nal.usda.gov/ndb/) was downloaded as BFPD ASCII CSV Files.
