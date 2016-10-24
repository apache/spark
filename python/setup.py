from __future__ import print_function
import os, sys
from setuptools import setup, find_packages
from shutil import copyfile

VERSION = '2.1.0-SNAPSHOT'
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "deps"
SPARK_HOME = os.path.abspath("../")
JARS_PATH = "%s/assembly/target/scala-2.11/jars/" % SPARK_HOME
EXAMPLES_PATH = "%s/examples/src/main/python" % SPARK_HOME
SCRIPTS_PATH = "%s/bin" % SPARK_HOME
SCRIPTS_TARGET = "%s/bin" % TEMP_PATH
JARS_TARGET = "%s/jars" % TEMP_PATH
EXAMPLES_TARGET = "%s/examples" % TEMP_PATH

# Check and see if we are under the spark path in which case we need to build the symlink farm.
in_spark = os.path.isfile("../core/src/main/scala/org/apache/spark/SparkContext.scala")
if (in_spark):
    # Construct links for setup
    try:
        os.mkdir(TEMP_PATH)
    except:
        print("Temp path for symlink to parent already exists %s" % TEMP_PATH, file=sys.stderr)
        exit(-1)

try:
    if (in_spark):
        os.symlink(JARS_PATH, JARS_TARGET)
        os.symlink(SCRIPTS_PATH, SCRIPTS_TARGET)
        os.symlink(EXAMPLES_PATH, EXAMPLES_TARGET)
    else:
        # We add find-spark-home.py to the bin directory we install so that pip installed PySpark
        # will search for SPARK_HOME with Python.
        copyfile("find-spark-home.py", SCRIPTS_TARGET + "/find-spark-home.py")

    if not os.path.isdir(SCRIPTS_TARGET):
        print("For packaging reasons you must first create a source dist and install that source dist.", file=sys.stderr)
        exit(-1)

    # Scripts directive requires a list of each script path and does not take wild cards.
    script_names = os.listdir(SCRIPTS_TARGET)
    scripts = map(lambda script: SCRIPTS_TARGET + "/" + script, script_names)
    scripts.append("find-spark-home.py")

    setup(
        name='pyspark',
        version=VERSION,
        description='Apache Spark Python API',
        author='Spark Developers',
        author_email='dev@spark.apache.org',
        url='https://github.com/apache/spark/tree/master/python',
        packages=['pyspark',
                  'pyspark.mllib',
                  'pyspark.ml',
                  'pyspark.sql',
                  'pyspark.streaming',
                  'pyspark.bin',
                  'pyspark.jars',
                  'pyspark.python.lib',
                  'pyspark.examples.src.main.python'],
        include_package_data=True,
        package_dir={
            'pyspark.jars': 'deps/jars',
            'pyspark.bin': 'deps/bin',
            'pyspark.python.lib': 'lib',
            'pyspark.examples.src.main.python': 'deps/examples',
        },
        package_data={
            'pyspark.jars': ['*.jar'],
            'pyspark.bin': ['*'],
            'pyspark.python.lib': ['*.zip'],
            'pyspark.examples.src.main.python': ['*.py', '*/*.py']},
        scripts=scripts,
        license='http://www.apache.org/licenses/LICENSE-2.0',
        install_requires=['py4j==0.10.3'],
        extras_require={
            'ml': ['numpy>=1.7'],
            'mllib': ['numpy<=1.7'],
            'sql': ['pandas']
        }
    )
finally:
    # We only cleanup the symlink farm if we were in Spark, otherwise we are installing rather than
    # packaging.
    if (in_spark):
        os.remove("%s/jars" % TEMP_PATH)
        os.remove("%s/bin" % TEMP_PATH)
        os.remove("%s/examples" % TEMP_PATH)
        os.rmdir(TEMP_PATH)
