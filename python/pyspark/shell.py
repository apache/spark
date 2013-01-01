"""
An interactive shell.
"""
import optparse  # I prefer argparse, but it's not included with Python < 2.7
import code
import sys

from pyspark.context import SparkContext


def main(master='local', ipython=False):
    sc = SparkContext(master, 'PySparkShell')
    user_ns = {'sc' : sc}
    banner = "Spark context avaiable as sc."
    if ipython:
        import IPython
        IPython.embed(user_ns=user_ns, banner2=banner)
    else:
        print banner
        code.interact(local=user_ns)


if __name__ == '__main__':
    usage = "usage: %prog [options] master"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option("-i", "--ipython", help="Run IPython shell",
                      action="store_true")
    (options, args) = parser.parse_args()
    if len(sys.argv) > 1:
        master = args[0]
    else:
        master = 'local'
    main(master, options.ipython)
