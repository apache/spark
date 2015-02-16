export AIRFLOW_CONFIG=~/airflow/unittests.cfg
rm airflow/www/static/coverage/*
nosetests --with-doctest --with-coverage --cover-html --cover-package=airflow --cover-html-dir=airflow/www/static/coverage
