#!/bin/bash

JAR_FILE="cs455-spark_2.11-1.0.jar"
OUT_DIR="/out"

function usage {
cat << EOF
    
    Usage: $0 -[b | f] -[j | e | k] -c

    -b : Submit Basic Job
    -f : First Order Job

    -j : Jasons HDFS
    -e : Evans HDFS 
    -k : Keegans HDFS  

    -c : Compile with SBT
    
EOF
    exit 1
}

function spark_runner {
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${JOB_NAME} ||: \
    && $SPARK_HOME/bin/spark-submit --master ${CORE_SPARK} \
    --deploy-mode cluster --class ${JOB_CLASS} target/scala-2.11/${JAR_FILE} ${INPUT} ${OUTPUT}
}

# Compile src 
if [[ $* = *-c* ]]; then
    sbt package
    LINES=`find . -name "*.scala" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
fi

# Set HDFS
case "$2" in

-j|--jason)
    CORE_HDFS="hdfs://providence:30210"
    CORE_SPARK="spark://salem:30136"
    ;;

-e|--evan)
    CORE_HDFS="hdfs://providence:30210"
    ;;

-k|--keegan)
    CORE_HDFS="hdfs://providence:30210"
    ;;

*) usage;
    ;;

esac

# Various Jobs to Execute
case "$1" in
    
-b|--basic) 
    JOB_NAME="basic"
    JOB_CLASS="Basic"
    INPUT="${CORE_HDFS}/local/tmp/sample_multiclass_classification_data.txt"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;
  
-f|--firstorder)
    JOB_NAME="firstorder"
    JOB_CLASS="cs455.spark.basic.FirstOrder"
    INPUT="${CORE_HDFS}/local/instacart/"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;
    
*) usage;
    ;;
    
esac
