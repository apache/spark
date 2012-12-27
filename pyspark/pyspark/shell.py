"""
An interactive shell.
"""
import argparse  # argparse is avaiable for Python < 2.7 through easy_install.
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
    parser = argparse.ArgumentParser()
    parser.add_argument("master", help="Spark master host (default='local')",
                        nargs='?', type=str, default="local")
    parser.add_argument("-i", "--ipython", help="Run IPython shell",
                        action="store_true")
    args = parser.parse_args()
    main(args.master, args.ipython)
