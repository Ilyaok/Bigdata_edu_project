#! /usr/bin/env bash

OUT_DIR="in2202212_mapreducetask1_result"$(date +"%s%6N")
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash ${OUT_DIR} > /dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="in2202212_mapreducetask1" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python mapper.py" \
    -combiner "python reducer.py" \
    -reducer "python reducer.py" \
    -input /data/ids \
    -output ${OUT_DIR} > /dev/null

hdfs dfs -cat ${OUT_DIR}/* | head -50
