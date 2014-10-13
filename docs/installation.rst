
Installation
------------
Installation should be quick and straightforwad. 

Debian packages
'''''''''''''''

::

    sudo apt-get install virtualenv python-dev
    sudo apt-get install libmysqlclient-dev mysql-server
    sudo apt-get g++

Required environment variable, add this to your .bashrc
'''''''''''''''''''''''''''''''''''''''''''''''''''''''

::

    export FLUX_HOME=~/Flux
    export PATH=$PATH:$FLUX_HOME/flux/bin

Create a python virtualenv
''''''''''''''''''''''''''

::

    virtualenv env # creates the environment
    source init.sh # activates the environment

Use pip to install the python packages required by Flux
'''''''''''''''''''''''''''''''''''''''''''''''''''''''

::

    pip install -r requirements.txt

Setup the metdata database
''''''''''''''''''''''''''

Here are steps to get started using MySQL as a backend for the metadata
database, though any backend supported by SqlAlquemy should work just
fine.

::

    $ mysql -u root -p 
    mysql> CREATE DATABASE flux;
    CREATE USER 'flux'@'localhost' IDENTIFIED BY 'flux';
    GRANT ALL PRIVILEGES ON flux.* TO 'flux'@'localhost';

Start the web server
''''''''''''''''''''

::

    flux webserver --port 8080
