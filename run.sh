#!/bin/bash

JAR_FILE="cs455-spark_2.11-1.0.jar"
OUT_DIR="/out"

function usage {
cat << EOF

    Usage: $0 -[b | f | t | j] -[j | e | k] -c

    Usage: $0 -[b | f] -[j | e | k] -c

    -b : Submit Basic Job
    -f : First Order Job
    -t : Top Food By Hour Job
    -j : Join Food Job

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
    --deploy-mode cluster --class ${JOB_CLASS} target/scala-2.11/${JAR_FILE} ${INPUT} ${SECOND_INPUT} ${OUTPUT} ${CORE_SPARK}
}


function yarn_runner {
	$HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${JOB_NAME} ||: \
    && $SPARK_HOME/bin/spark-submit --master yarn \
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
    INSTACART="local/instacart"
    USDA="local/bfpd"
    ;;

-e|--evan)
    CORE_HDFS="hdfs://juneau:4921"
    CORE_SPARK="spark://lansing:25432"
    INSTACART="cs455/food/instacart"
    USDA="cs455/food/usda"
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
    INPUT="${CORE_HDFS}/${INSTACART}/"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;

-t|--topbyhour)
	JOB_NAME="topfoodbyhour"
	JOB_CLASS="cs455.spark.basic.MostTimeOfDay"
	INPUT="${CORE_HDFS}/${INSTACART}/"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;

-j|--joinedproducts)
	JOB_NAME="joinedproducts"
	JOB_CLASS="cs455.spark.basic.BestFoodMatch"
	INPUT="${CORE_HDFS}/${INSTACART}/"
	SECOND_INPUT="${CORE_HDFS}/${USDA}/"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;
    
*) usage;
    ;;

esac
