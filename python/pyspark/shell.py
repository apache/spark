"""
An interactive shell.

This file is designed to be launched as a PYTHONSTARTUP script.
"""
import os
import pyspark
from pyspark.context import SparkContext


sc = SparkContext(os.environ.get("MASTER", "local"), "PySparkShell")
print "Spark context avaiable as sc."

# The ./pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
# which allows us to execute the user's PYTHONSTARTUP file:
_pythonstartup = os.environ.get('OLD_PYTHONSTARTUP')
if _pythonstartup and os.path.isfile(_pythonstartup):
    execfile(_pythonstartup)
