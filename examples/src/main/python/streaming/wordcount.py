import sys
from operator import add

from pyspark.conf import SparkConf
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <directory>"
        exit(-1)
    conf = SparkConf()
    conf.setAppName("PythonStreamingWordCount")

    ssc = StreamingContext(conf=conf, duration=Seconds(1))

    lines = ssc.textFileStream(sys.argv[1])
    words = lines.flatMap(lambda line: line.split(" "))
    mapped_words = words.map(lambda x: (x, 1))
    count = mapped_words.reduceByKey(add)
    
    count.pyprint()
    ssc.start()
    ssc.awaitTermination()
