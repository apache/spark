export AIRFLOW_CONFIG=~/airflow/unittests.cfg
nosetests --with-doctest --with-coverage --cover-html --cover-package=airflow #--nocapture
#python -m SimpleHTTPServer 8002
