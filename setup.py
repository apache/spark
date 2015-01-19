from setuptools import setup, find_packages

# To generate install_requires from requirements.txt
# from pip.req import parse_requirements
# reqs = (str(ir.req) for ir in parse_requirements('requirements.txt'))

setup(
    name='airflow',
    description='programmaticaly author, schedule and monitor data pipelines',
    version='0.1',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['airflow/bin/airflow'],
    install_requires=[
        'celery', 'chartkick',
        'flask', 'flask-admin', 'flask-bootstrap', 'flask-cache',
        'flask-login', 'hive-thrift-py', 'jinja2',
        'librabbitmq',
        'markdown', 'mysql-python',
        'pandas',
        'pygments', 'pyhive',
        'python-dateutil', 'requests', 'setproctitle',
        'snakebite',
        'sphinx', 'sphinx-rtd-theme', 'Sphinx-PyPI-upload',
        'sqlalchemy', 'thrift', 'tornado'
    ],
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/mistercrunch/Airflow',
    download_url='https://github.com/mistercrunch/Airflow/tarball/0.1',
)
