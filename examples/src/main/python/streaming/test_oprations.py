import sys
from operator import add

from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("PythonStreamingNetworkWordCount")
    ssc = StreamingContext(conf=conf, duration=Seconds(1))

    test_input = ssc._testInputStream([1,2,3])
    class buff:
        pass
   
    fm_test = test_input.map(lambda x: (x, 1))
    fm_test.test_output(buff)

    ssc.start()
    while True:
        ssc.awaitTermination(50)
        try:
            buff.result
            break
        except AttributeError:
            pass

    ssc.stop()
    print buff.result
