#!/bin/bash

# Example usage:
# ./submit.sh SimpleApp target/spark-intro-1.0-SNAPSHOT-jar-with-dependencies.jar 
CLASS=$1
JAR_FILE=$2
REMINDER=${@:3}

CLASSPAT1=$CLASSPAT1,$HADOOP_HOME/*:$HADOOP_HOME/lib/*
CLASSPAT1=$CLASSPAT1,$HADOOP_HOME/../hadoop-mapreduce/*,$HADOOP_HOME/../hadoop-mapreduce/lib/*
CLASSPAT1=$CLASSPAT1,$HADOOP_HOME/../hadoop-yarn/*,$HADOOP_HOME/../hadoop-yarn/lib/*
CLASSPAT1=$CLASSPAT1,$HADOOP_HOME/../hadoop-hdfs/*,$HADOOP_HOME/../hadoop-hdfs/lib/*
CLASSPAT1=$CLASSPAT1,$SPARK_HOME/assembly/lib/*

CLASSPATH=""
for i in $HADOOP_HOME $HADOOP_HOME/../hadoop-mapreduce $HADOOP_HOME/../hadoop-yarn $HADOOP_HOME/../hadoop-hdfs $SPARK_HOME/assembly/lib ;
do
 CLASSPATH="`ls -1 ${i}/*.jar | tr "\\n" ","`${CLASSPATH}"
done;


CLASSPATH=`echo $CLASSPATH | rev | cut -c2- | rev`

#echo $CLASSPATH
#echo $CLASSPAT1

spark-submit --class ${CLASS}  --jars ${CLASSPATH} --master local ${JAR_FILE} ${REMINDER} 
