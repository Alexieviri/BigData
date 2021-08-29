#!/usr/bin/env bash
set -x

HADOOP_STREAMING_JAR=/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming.jar
INPUT_PATH=$1
OUT_PATH=$2
JOB_NAME=$3

hdfs dfs -rm -r -skipTrash $OUT_PATH
yarn jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name=$JOB_NAME \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py"  \
    -reducer "python3 reducer.py" \
    -numReduceTasks 3 \
    -input $INPUT_PATH \
    -output $OUT_PATH

 hdfs dfs -cat hw02_mr_data_ids/part-00000 | head -n 50