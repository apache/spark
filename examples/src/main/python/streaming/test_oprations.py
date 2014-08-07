import sys
from operator import add

from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("PythonStreamingNetworkWordCount")
    ssc = StreamingContext(conf=conf, duration=Seconds(1))
    ssc.checkpoint("/tmp/spark_ckp")

    test_input = ssc._testInputStream([[1],[1],[1]])
#    ssc.checkpoint("/tmp/spark_ckp")
    fm_test = test_input.flatMap(lambda x: x.split(" "))
    mapped_test = fm_test.map(lambda x: (x, 1))


    mapped_test.print_()
    ssc.start()
#    ssc.awaitTermination()
#    ssc.stop()
