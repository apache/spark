
Installation
------------
Installation should be quick and straightforwad. 

Debian packages
'''''''''''''''

::

    sudo apt-get install virtualenv python-dev
    sudo apt-get install libmysqlclient-dev mysql-server
    sudo apt-get g++

Mac setup
'''''''''''''''

::

    # Install mysql
    brew install mysql
    # Start mysql
    mysql.server start

    # Install python package managers
    sudo easy_install pip
    sudo pip install virtualenv
    

Required environment variable, add this to your .bashrc or .bash_profile
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

::

    export FLUX_HOME=~/Flux
    export PATH=$PATH:$FLUX_HOME/flux/bin
    # also run it, or source it, or start a new shell
    source ~/.bashrc

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

    # If :
    $ mysql -u root -p 
    mysql> 
    CREATE DATABASE flux;
    CREATE USER 'flux'@'localhost' IDENTIFIED BY 'flux';
    GRANT ALL PRIVILEGES ON flux.* TO 'flux'@'localhost';

Get things started
''''''''''''''''''''

::
    # Creating the necessary tables in the database
    flux initdb

    # Start the web server!
    flux webserver --port 8080
