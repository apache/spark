#!/usr/bin/env python

from setuptools import setup

exec(compile(open("pyspark/pyspark_version.py").read(), 
   "pyspark/pyspark_version.py", 'exec'))
VERSION = __version__

setup(name='pyspark',
    version=VERSION,
    description='Apache Spark Python API',
    author='Spark Developers',
    author_email='dev@spark.apache.org',
    url='https://github.com/apache/spark/tree/master/python',
    packages=['pyspark', 'pyspark.mllib', 'pyspark.ml', 'pyspark.sql', 'pyspark.streaming'],
    install_requires=['numpy>=1.7', 'py4j==0.8.2.1', 'pandas'],
    license='http://www.apache.org/licenses/LICENSE-2.0',
    )
