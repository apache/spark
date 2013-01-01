import sys
from random import random
from operator import add

from pyspark import SparkContext


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print >> sys.stderr, \
            "Usage: PythonPi <master> [<slices>]"
        exit(-1)
    sc = SparkContext(sys.argv[1], "PythonPi")
    slices = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    n = 100000 * slices
    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 < 1 else 0
    count = sc.parallelize(xrange(1, n+1), slices).map(f).reduce(add)
    print "Pi is roughly %f" % (4.0 * count / n)
