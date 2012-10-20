"""
An interactive shell.
"""
import code
import sys

from pyspark.context import SparkContext


def main(master='local'):
    sc = SparkContext(master, 'PySparkShell')
    print "Spark context available as sc."
    code.interact(local={'sc': sc})


if __name__ == '__main__':
    if len(sys.argv) > 1:
        master = sys.argv[1]
    else:
        master = 'local'
    main(master)
