from setuptools import setup

import pyspark


setup(
    name='pyspark',
    version=pyspark.__version__,
    packages=[
        'pyspark',
        'pyspark.mllib',
        'pyspark.ml',
        'pyspark.sql',
        'pyspark.streaming',
    ],
    install_requires=[
        'py4j==0.9',
    ],
    extras_require={
        'ml': ['numpy>=1.7'],
        'sql': ['pandas'],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Software Development',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'License :: OSI Approved :: Apache Software License',
    ],
    description='Apache Spark Python API',
    keywords='spark pyspark',
    author='Spark Developers',
    author_email='dev@spark.apache.org',
    url='https://github.com/apache/spark/tree/master/python',
    license='http://www.apache.org/licenses/LICENSE-2.0',
)
