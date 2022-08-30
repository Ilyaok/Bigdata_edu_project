import os
import hyperloglog
import time
from pyspark import SparkContext
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

finished = False
printed = False


def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True


def count_and_print_at_the_end(rdd):
    global printed
    l = rdd.collect()[0]
    if finished and not printed:
        seg_windows_hll = hyperloglog.HyperLogLog(0.01)
        seg_firefox_hll = hyperloglog.HyperLogLog(0.01)
        seg_iphone_hll = hyperloglog.HyperLogLog(0.01)

        for line in l['seg_windows']:
            seg_windows_hll.add(line)

        for line in l['seg_firefox']:
            seg_firefox_hll.add(line)

        for line in l['seg_iphone']:
            seg_iphone_hll.add(line)

        d = {'seg_windows': len(seg_windows_hll),
             'seg_firefox': len(seg_firefox_hll),
             'seg_iphone': len(seg_iphone_hll),
             }
        for k in sorted(d, key=d.get, reverse=True):
            print("{}\t{}".format(k, d[k]))


def aggregator(values, old):
    if old is None:
        d = {'seg_windows': [],
             'seg_iphone': [],
             'seg_firefox': []
             }
        return d
    for line in values:
        user_info = line.split('\t')
        user_id = user_info[0]
        user_agent = user_info[1]
        if 'Windows' in user_agent:
            old['seg_windows'].append(user_id)
        if 'iPhone' in user_agent:
            old['seg_iphone'].append(user_id)
        if 'Firefox' in user_agent:
            old['seg_firefox'].append(user_id)
    return old


dstream.foreachRDD(set_ending_flag)

dstream \
    .flatMap(lambda line: line.split('\n')) \
    .map(lambda line: ('res', line)) \
    .updateStateByKey(aggregator) \
    .map(lambda x: x[1]) \
    .foreachRDD(count_and_print_at_the_end)


ssc.checkpoint('./checkpoint{}'
               .format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))  # checkpoint for storing current state
ssc.start()
while not finished:
    time.sleep(0.01)
ssc.stop()
