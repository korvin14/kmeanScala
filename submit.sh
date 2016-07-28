#!/bin/bash

JAR=target/kmeans-0.0.1-SNAPSHOT.jar
OUT=kmeans.out
IN=order-full.csv

hdfs dfs -rm -r $OUT
spark-submit --verbose --master yarn-cluster --class bigdata.ml.App $JAR $IN 10 100 $OUT

