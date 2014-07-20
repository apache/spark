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
    conf.set("spark.default.parallelism", 1)

# still has a bug
#    ssc = StreamingContext(appName="PythonStreamingWordCount", duration=Seconds(1))
    ssc = StreamingContext(conf=conf, duration=Seconds(1))

    lines = ssc.textFileStream(sys.argv[1])
    fm_lines = lines.flatMap(lambda x: x.split(" "))
    filtered_lines = fm_lines.filter(lambda line: "Spark" in line)
    mapped_lines = fm_lines.map(lambda x: (x, 1))
    reduced_lines = mapped_lines.reduceByKey(add)
    
    fm_lines.pyprint()
    filtered_lines.pyprint()
    mapped_lines.pyprint()
    reduced_lines.pyprint()
    ssc.start()
    ssc.awaitTermination()
