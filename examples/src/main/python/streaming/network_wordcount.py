import sys
from operator import add

from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: wordcount <hostname> <port>"
        exit(-1)
    conf = SparkConf()
    conf.setAppName("PythonStreamingNetworkWordCount")
    ssc = StreamingContext(conf=conf, duration=Seconds(1))

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    fm_lines = lines.flatMap(lambda x: x.split(" "))
    mapped_lines = fm_lines.map(lambda x: (x, 1))
    reduced_lines = mapped_lines.reduceByKey(add)

    reduced_lines.pyprint()
    count_lines = mapped_lines.count()
    count_lines.pyprint()
    ssc.start()
    ssc.awaitTermination()
