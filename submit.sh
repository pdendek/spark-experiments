#!/bin/bash

# Example usage:
# ./submit.sh SimpleApp target/spark-intro-1.0-SNAPSHOT-jar-with-dependencies.jar 
CLASS=$1
JAR_FILE=$2
REMINDER=${@:3}

spark-submit --class ${CLASS} --master local ${JAR_FILE} ${REMINDER} 
