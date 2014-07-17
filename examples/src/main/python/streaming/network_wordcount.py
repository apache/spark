import sys
from operator import add

from pyspark.streaming.context import StreamingContext
from pyspark.streaming.duration import *

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print >> sys.stderr, "Usage: wordcount <hostname> <port>"
        exit(-1)
    ssc = StreamingContext(appName="PythonStreamingNetworkWordCount", duration=Seconds(1))

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    fm_lines = lines.flatMap(lambda x: x.split(" "))
    filtered_lines = fm_lines.filter(lambda line: "Spark" in line)
    mapped_lines = fm_lines.map(lambda x: (x, 1))
    
    fm_lines.pyprint()
    filtered_lines.pyprint()
    mapped_lines.pyprint()
    ssc.start()
    ssc.awaitTermination()
