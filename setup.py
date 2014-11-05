import os
from distutils.core import setup
from pip.req import parse_requirements

# Reading the requirements from pip's requirements.txt
reqs = (str(ir.req) for ir in parse_requirements('requirements.txt'))

setup(
    name='airflow',
    version='0.1',
    packages=[
        'airflow',
        'airflow.operators',
        'airflow.executors',
        'airflow.hooks',
        'airflow.www',
        'dags',
    ],
    scripts=['airflow/bin/airflow'],
    install_requires=reqs,
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/mistercrunch/Airflow',
)
