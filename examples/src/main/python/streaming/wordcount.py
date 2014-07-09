import sys
from operator import add

<<<<<<< HEAD
from pyspark.conf import SparkConf
=======
>>>>>>> initial commit for pySparkStreaming
from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <directory>"
        exit(-1)
<<<<<<< HEAD
    conf = SparkConf()
    conf.setAppName("PythonStreamingWordCount")

    ssc = StreamingContext(conf=conf, duration=Seconds(1))

    lines = ssc.textFileStream(sys.argv[1])
    words = lines.flatMap(lambda line: line.split(" "))
    mapped_words = words.map(lambda x: (x, 1))
    count = mapped_words.reduceByKey(add)
    
    count.pyprint()
=======
    ssc = StreamingContext(appName="PythonStreamingWordCount", duration=Seconds(1))

    lines = ssc.textFileStream(sys.argv[1])
    fm_lines = lines.flatMap(lambda x: x.split(" "))
    filtered_lines = fm_lines.filter(lambda line: "Spark" in line)
    mapped_lines = fm_lines.map(lambda x: (x, 1))
    
    fm_lines.pyprint()
    filtered_lines.pyprint()
    mapped_lines.pyprint()
>>>>>>> initial commit for pySparkStreaming
    ssc.start()
    ssc.awaitTermination()
