from setuptools import setup, find_packages

# Kept manually in sync with airflow.__version__
version = '0.2.3.4'

setup(
    name='airflow',
    description='Programmatically author, schedule and monitor data pipelines',
    version=version,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['airflow/bin/airflow'],
    install_requires=[
        'celery>=3.1.17',
        'chartkick>=0.4.2',
        'dill>=0.2.2',
        'flask>=0.10.1',
        'flask-admin>=1.0.9',
        'flask-bootstrap>=3.3.0.1',
        'flask-cache>=0.13.1',
        'flask-login>=0.2.11',
        'hive-thrift-py>=0.0.1',
        'jinja2>=2.7.3',
        'librabbitmq>=1.6.1',
        'markdown>=2.5.2',
        'mysql-python>=1.2.5',
        'pandas>=0.15.2',
        'pygments>=2.0.1',
        'pysmbclient>=0.1.3',
        'pyhive>=0.1.3',
        'python-dateutil>=2.3',
        'requests>=2.5.1',
        'setproctitle>=1.1.8',
        'snakebite>=2.4.13',
        'sphinx>=1.2.3',
        'sphinx-rtd-theme>=0.1.6',
        'Sphinx-PyPI-upload>=0.2.1',
        'sqlalchemy>=0.9.8',
        'thrift>=0.9.2',
        'tornado>=4.0.2',
    ],
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/mistercrunch/Airflow',
    download_url=(
        'https://github.com/mistercrunch/Airflow/tarball/' + version),
)
