import sys

from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print >> sys.stderr, "Usage: wordcount <directory>"
        exit(-1)

    ssc = StreamingContext(appName="PythonStreamingWordCount", duration=Seconds(1))

    lines = ssc.textFileStream(sys.argv[1])
    words = lines.flatMap(lambda line: line.split(" "))
    mapped_words = words.map(lambda x: (x, 1))
    count = mapped_words.reduceByKey(lambda a, b: a+b)
    count.pyprint()

    ssc.start()
    ssc.awaitTermination()
