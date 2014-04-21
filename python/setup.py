#!/usr/bin/env python

from setuptools import setup

exec(compile(open("pyspark/pyspark_version.py").read(), 
	"pyspark/pyspark_version.py", 'exec'))
VERSION = __version__

setup(name = 'pyspark',
	version = VERSION,
	description = 'Apache Spark Python API',
	author = 'Prabin Banka',
	author_email = 'prabin.banka@imaginea.com',
	url = 'https://github.com/apache/spark/tree/master/python',
	packages = ['pyspark', 'pyspark.mllib'],
	data_files = [('pyspark', ['pyspark/pyspark_version.py'])],
	install_requires = ['numpy>=1.7', 'py4j==0.8.1'],
	license = 'http://www.apache.org/licenses/LICENSE-2.0',
	)
