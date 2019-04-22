#!/usr/bin/env bash

function spark_runner {
    ${SPARK_HOME}/bin/spark-submit --master ${CORE_SPARK} \
    --deploy-mode cluster --class ${JOB_CLASS} build/libs/distributed-spark-analysis.jar ${INPUT}
}

CORE_HDFS="hdfs://austin:5678"
CORE_SPARK="spark://atlanta:7077"

case "$1" in

    -b|--basic)
        JOB_NAME="basic"
        JOB_CLASS="Basic"
        INPUT="resources/sample_multiclass_classification_data.txt"
        spark_runner
        ;;

    -p|--products)
        JOB_NAME="products"
        JOB_CLASS="Products"
        INPUT="/local/instacart/products.csv"
        spark_runner
        ;;

esac


