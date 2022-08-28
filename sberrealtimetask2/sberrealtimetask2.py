import os
import hyperloglog
import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from hdfs import Config
import subprocess


client = Config().get_client()
nn_address = subprocess\
    .check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")

sc = SparkContext(master='yarn-client')

# Preparing base RDD with the input data.
DATA_PATH = "/data/realtime/uids"

batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)[:30]]

# Creating QueueStream to emulate realtime data generating
BATCH_TIMEOUT = 2  # Timeout between batch generation
ssc = StreamingContext(sc, BATCH_TIMEOUT)
dstream = ssc.queueStream(rdds=batches)

seg_iphone = hyperloglog.HyperLogLog(0.01)
seg_firefox = hyperloglog.HyperLogLog(0.01)
seg_windows = hyperloglog.HyperLogLog(0.01)

finished = False
collected = False


def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True


def parse_to_hll(rdd):
    global collected
    global seg_iphone
    global seg_firefox
    global seg_windows
    rdd.count()
    for line in rdd.collect():
        user_id = line.split('\t')[0]
        user_agent = line.split('\t')[1]
        if 'Windows' in user_agent:
            seg_windows.add(user_id)
        if 'iPhone' in user_agent:
            seg_iphone.add(user_id)
        if 'Firefox' in user_agent:
            seg_firefox.add(user_id)
        collected = True


dstream.foreachRDD(set_ending_flag)
dstream.foreachRDD(parse_to_hll)

ssc.checkpoint('./checkpoint{}'
               .format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))  # checkpoint for storing current state
ssc.start()
while not finished:
    time.sleep(0.01)
ssc.stop()

d = {'seg_windows': len(seg_windows), 'seg_iphone': len(seg_iphone), 'seg_firefox': len(seg_firefox)}
for k in sorted(d, key=d.get, reverse=True):
    print("{}\t{}".format(k, d[k]))
