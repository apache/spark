.. image:: img/pin_large.png
    :width: 70

Airflow's Documentation
================================

Airflow is a system to programmaticaly author, schedule and monitor data pipelines. 

Use the Airflow library to define workflows as directed acyclic graphs (DAGs) of data related tasks. Command line utilities make it easy to run parts of workflows interactively, and commiting pipelines into production is all it takes for the master scheduler to run the pipelines with the schedule and dependencies specified.

The Airflow UI make it easy to visualize pipelines running in production, monitor progress and troubleshoot issues when needed.

Principles
----------

-  **Dynamic:** Airflow has intrinsec support for dynamic pipeline
   generation: you can write pipelines, as well as writing code that
   defines pipeline.
-  **Interactivity:** the libraries are intuitive so that writting /
   testing and monitoring pipelines from a shell (or an iPython
   Notebook) should just flow
-  **Extensible:** easily define your own operators, executors and
   extend the library so that it fits the level of abstraction that
   suits your environment
-  **Elegant:** make your commands dynamic with the power of the
   **Jinja** templating engine

Content
-------
.. toctree::
    :maxdepth: 4

    installation
    ui
    tutorial
    concepts
    profiling
    cli
    code
