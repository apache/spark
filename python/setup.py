from __future__ import print_function
import os, sys
from setuptools import setup, find_packages

VERSION = '2.1.0-SNAPSHOT'
# A temporary path so we can access above the Python project root and fetch scripts and jars we need
TEMP_PATH = "deps"
JARS_PATH = "%s/assembly/target/scala-2.11/jars/*.jar" % TEMP_PATH
SCRIPTS = "%s/bin/*" % TEMP_PATH

# Construct links for setup
try:
    os.symlink("../", TEMP_PATH)
except:
    print("temp path for symlink to parent already exists %s" % TEMP_PATH, file=sys.stderr)

try:
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
                  'pyspark.streaming'],
        include_package_data=True,
        package_data={
            'pyspark': [JARS_PATH]},
        scripts=[SCRIPTS],
        license='http://www.apache.org/licenses/LICENSE-2.0',
        install_requires=['py4j==0.10.3'],
        extras_require={
            'ml': ['numpy>=1.7'],
            'mllib': ['numpy<=1.7'],
            'sql': ['pandas']
        }
    )
finally:
    os.remove(TEMP_PATH)
