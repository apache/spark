import os
from setuptools import setup, find_packages

VERSION = '2.1.0-SNAPSHOT'
JARS_PATH = "../assembly/target/scala-2.11/jars/*.jar"
SCRIPTS = "../bin/*"
TEMP_PATH = "deps"

# Construct links for setup
if os.path.isfile(TEMP_PATH):
    os.remove(TEMP_PATH)
try:
    os.symlink("../", TEMP_PATH)

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
