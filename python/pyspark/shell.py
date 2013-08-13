"""
An interactive shell.

This file is designed to be launched as a PYTHONSTARTUP script.
"""
import os
import pyspark
from pyspark.context import SparkContext

# this is the equivalent of ADD_JARS
add_files = os.environ.get("ADD_FILES").split(',') if os.environ.get("ADD_FILES") != None else None

sc = SparkContext(os.environ.get("MASTER", "local"), "PySparkShell", pyFiles=add_files)
print "Spark context avaiable as sc."

if add_files != None:
    print "Adding files: [%s]" % ", ".join(add_files)

# The ./pyspark script stores the old PYTHONSTARTUP value in OLD_PYTHONSTARTUP,
# which allows us to execute the user's PYTHONSTARTUP file:
_pythonstartup = os.environ.get('OLD_PYTHONSTARTUP')
if _pythonstartup and os.path.isfile(_pythonstartup):
    execfile(_pythonstartup)
