import sys
from operator import add

from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    conf = SparkConf()
    conf.setAppName("PythonStreamingNetworkWordCount")
    ssc = StreamingContext(conf=conf, duration=Seconds(1))
    class Buff:
        result = list()
        pass
    Buff.result = list()

    test_input = ssc._testInputStream([range(1,4), range(4,7), range(7,10)])
   
    fm_test = test_input.map(lambda x: (x, 1))
    fm_test.pyprint()
    fm_test._test_output(Buff.result)

    ssc.start()
    while True:
        ssc.awaitTermination(50)
        if len(Buff.result) == 3:
            break

    ssc.stop()
    print Buff.result

