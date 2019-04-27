#!/bin/bash

OUT_DIR="/out"

function usage {
cat << EOF

    Usage: $0 -[b | f | t | j | u] -[j | e | k] -c

    -b : Submit Basic Job
    -f : First Order Job
    -t : Top Food By Hour Job
    -j : Join Food Job
    -u : Sugar Orders Per User

    -j : Jasons Profile
    -e : Evans Profile
    -k : Keegans Profile

    -c : Compile with SBT

EOF
    exit 1
}

function spark_runner {
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${JOB_NAME} ||: \
    && $SPARK_HOME/bin/spark-submit --master ${CORE_SPARK} --deploy-mode cluster \
    --class ${JOB_CLASS} ${JAR_FILE} ${INPUT} ${SECOND_INPUT} ${THIRD_INPUT} ${OUTPUT} ${CORE_SPARK}
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
    JAR_FILE="target/scala-2.11/cs455-spark_2.11-1.0.jar"
    CORE_HDFS="hdfs://providence:30210"
    CORE_SPARK="spark://salem:30136"
    INSTACART="local/instacart"
    USDA="local/bfpd"
    LINK="local/insta-bfpd.csv"
    ;;

-e|--evan)
    JAR_FILE="target/scala-2.11/cs455-spark_2.11-1.0.jar"
    CORE_HDFS="hdfs://juneau:4921"
    CORE_SPARK="spark://lansing:25432"
    INSTACART="cs455/food/instacart"
    USDA="cs455/food/usda"
    ;;

-k|--keegan)
    JAR_FILE="build/libs/distributed-spark-analysis.jar"
    CORE_HDFS="hdfs://austin:5678"
    CORE_SPARK="spark://washington-dc:7077"
    INSTACART="local/instacart"
    USDA="local/bfpd"
    LINK="local/insta-bfpd.csv"
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
    SECOND_INPUT="${CORE_HDFS}/${USDA}/"
    THIRD_INPUT="${CORE_HDFS}/${LINK}/"
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
    
-u|--userintake)
    JOB_NAME="userintake"
    JOB_CLASS="cs455.spark.neutrians.UserIntake"
    INPUT="${CORE_HDFS}/${INSTACART}/"
    SECOND_INPUT="${CORE_HDFS}/${USDA}/"
    THIRD_INPUT="${CORE_HDFS}/${LINK}/"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;

-s|--similarusers)
    JOB_NAME="similarusers"
    JOB_CLASS="cs455.spark.basic.SimilarUsers"
    INPUT="${CORE_HDFS}/${INSTACART}/"
    SECOND_INPUT="${CORE_HDFS}/${USDA}/"
    THIRD_INPUT="${CORE_HDFS}/${LINK}/"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;
    
*) usage;
    ;;

esac
