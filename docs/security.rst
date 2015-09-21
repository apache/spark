Security
========

Airflow has initial support for Kerberos. This means that airflow can renew kerberos
tickets for itself and store it in the ticket cache. The hooks and dags can make use of ticket
to authenticate against kerberized services.

Limitations
-----------

Please note that at this time not all hooks have been adjusted to make use of this functionality yet.
Also it does not integrate kerberos into the web interface and you will have to rely on network
level security for now to make sure your service remains secure.

Celery integration has not been tried and tested yet. However if you generate a key tab for every host
and launch a ticket renewer next to every worker it will most likely work.

Enabling security
-----------------

To enable kerberos you will need to generate a (service) key tab.

.. code-block:: bash
    # in the kadmin.local or kadmin shell, create the airflow principal
    kadmin:  addprinc -randkey airflow/fully.qualified.domain.name@YOUR-REALM.COM

    # Create the airflow keytab file that will contain the airflow principal
    kadmin:  xst -norandkey -k airflow.keytab airflow/fully.qualified.domain.name

Now store this file in a location where the airflow user can read it (chmod 600). And then add the following to
your airflow.cfg

.. code-block:: bash
    [security]
    enabled = True
    keytab = /etc/airflow/airflow.keytab
    reinit_frequency = 3600
    principal = airflow

Launch the ticket renewer by

.. code-block:: bash

    # run ticket renewer
    airflow kerberos

Using kerberos authentication
'''''''''''''''''''''''''''''

The hive hook has been updated to take advantage of kerberos authentication. To allow your DAGs to use it simply
update the connection details with, for example:

.. code-block:: bash
    { "use_beeline": true, "principal": "hive/_HOST@EXAMPLE.COM"}

Adjust the principal to your settings. The _HOST part will be replaced by the fully qualified domain name of
the server.