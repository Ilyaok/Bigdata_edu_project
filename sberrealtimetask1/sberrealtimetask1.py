# В процессе решения!
# nc -lk 127.0.0.1 -p 10000

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext(master='local[4]')
ssc = StreamingContext(sc, batchDuration=10)

dstream = ssc.socketTextStream(hostname='localhost', port=10000)

result = dstream.filter(bool).count()

result.pprint()

ssc.start()
ssc.awaitTermination()

