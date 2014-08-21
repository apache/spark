import sys

from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: wordcount <hostname> <port>"
        exit(-1)
    ssc = StreamingContext(appName="PythonStreamingNetworkWordCount", 
                           duration=Seconds(1))

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a,b: a+b)
    counts.pyprint()

    ssc.start()
    ssc.awaitTermination()
