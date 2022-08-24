#! /usr/bin/env python3

import re
from pyspark import SparkContext, SparkConf

config = SparkConf()\
    .setAppName('in2202212_sbersparktask1')\
    .setMaster('yarn-client')\
    .set('spark.kryoserializer.buffer.max', 2000)
spark_context = SparkContext(conf=config)

rdd = spark_context.textFile('/data/wiki/en_articles_part')

rdd1 = rdd.map(lambda x: x.strip().lower())
rdd2 = rdd1.filter(lambda x: 'narodnaya' in x)
rdd3 = rdd2.flatMap(lambda x: x.split(" "))
rdd4 = rdd3.map(lambda x: re.sub("^\W+|\W+$", "", x))
rdd5 = rdd4.zipWithIndex()
rdd6 = rdd5.map(lambda x: (x[1], x[0]))
rdd7 = rdd4.zipWithIndex()
rdd8 = rdd7.map(lambda x: (x[1] - 1, x[0]))
rdd9 = rdd6.join(rdd8)
rdd10 = rdd9.map(lambda x: x[1])
rdd11 = rdd10.filter(lambda x: x[0] == 'narodnaya')
rdd12 = rdd11.map(lambda x:(x[0]+"_"+x[1], 1))
rdd13 = rdd12.reduceByKey(lambda a, b: a + b)
rdd14 = rdd13.sortBy(lambda x: x[0])

res = rdd14.collect()

for res_with_count in res:
    result_string = '{bigram} {cnt}'\
        .format(bigram=res_with_count[0],
                cnt=res_with_count[1])
    print(result_string)
