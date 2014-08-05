import sys
from operator import add

from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("PythonStreamingNetworkWordCount")
    ssc = StreamingContext(conf=conf, duration=Seconds(1))

    test_input = ssc._testInputStream([1,1,1,1])
    mapped = test_input.map(lambda x: (x, 1))
    mapped.pyprint()

    ssc.start()
#    ssc.awaitTermination()
#    ssc.stop()
