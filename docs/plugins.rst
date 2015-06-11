Plugins
=======

Airflow has a simple plugin manager built-in that can integrate external
features at its core by simply dropping files in your 
``$AIRFLOW_HOME/plugins`` folder.

The python modules in the ``plugins`` folder get imported, 
and **hooks**, **operators**, **macros**, **executors** and web **views** 
get integrated to Airflow's main collections and become available for use.

Objects
-------

* Classes derived from ``BaseOperator`` land in ``airflow.operators``
* Classes derived from ``BaseHook`` land in ``airflow.hooks``
* Classes derived from ``BaseExecutor`` land ``airflow.executors``
* object created from a class derived from ``airflow.PluginView`` get integrated in the Flask app
* object created from ``AirflowMacroPlugin(namespace="foo")`` land in ``airflow.macros.foo``
* Files located in subfolders named ``templates`` folders become available when rendering pages
* (upcoming) CLI subcommands


What for?
---------

Airflow offers a generic toolbox for working with data. Different 
organizations have different stacks and different needs. Using Airflow
plugins can be a way for companies to customize their Airflow installation
to reflect their ecosystem.

Plugins can be used as an easy way to write, share and activate new sets of 
features.

There's also a need for a set of more complex application to interact with 
different flavors of data and metadata.

Examples:

* A set of tools to parse Hive logs and expose Hive metadata (CPU /IO / phases/ skew /...)
* An anomaly detection framework, allowing people to collect metrics, set thresholds and alerts
* An auditing tool, helping understand who accesses what
* A config-driven SLA monitoring tool, allowing you to set monitored tables and at what time
  they should land, alert people and exposes visualization of outages
* ...


Why build on top Airflow?
-------------------------

Airflow has many components that can be reused when building an application:

* A web server you can use to render your views
* A metadata database to store your models
* Access to your database, and knowledge of how to connect to them
* An array of workers that can allow your application to push workload to
* Airflow is deployed, you can just piggy back on it's deployment logistics
* Basic charting capabilities, underlying libraries and abstractions


Example
-------

The code bellow defines a plugin that injects a set of dummy object
definitions in Airflow. 

.. code:: python

    # Importing base classes that we need to derive
    from airflow.hooks.base_hook import BaseHook
    from airflow.models import  BaseOperator
    from airflow.executors.base_executor import BaseExecutor
    from airflow import AirflowViewPlugin, AirflowMacroPlugin
    from flask_admin import expose

    # Will show up under airflow.hooks.PluginHook
    class PluginHook(BaseHook):
        pass

    # Will show up under airflow.operators.PluginOperator
    class PluginOperator(BaseOperator):
        pass

    # Will show up under airflow.executors.PluginExecutor
    class PluginExecutor(BaseExecutor):
        pass

    # Shows up in the UI in menu -> Plugins -> Test
    class TestView(AirflowViewPlugin):
        @expose('/')
        def query(self):
            return self.render("test.html", content="Hello galaxy!")
    v = TestView(category="Plugins", name="Test")


    # Available as other macros in templates {{ macros.foo_plugin.success() }}
    def success():
        return "Success!"
    obj = AirflowMacroPlugin(namespace="foo_plugin")
    obj.success = success

