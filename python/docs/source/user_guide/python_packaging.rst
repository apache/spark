..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


=========================
Python Package Management
=========================

When you want to run your PySpark application on a cluster such as YARN, Kubernetes, Mesos, etc., you need to make
sure that your code and all used libraries are available on the executors.

As an example, let's say you may want to run the `Pandas UDF examples <sql/arrow_pandas.rst#series-to-scalar>`_.
As it uses pyarrow as an underlying implementation we need to make sure to have pyarrow installed on each executor
on the cluster. Otherwise you may get errors such as ``ModuleNotFoundError: No module named 'pyarrow'``.

Here is the script ``app.py`` from the previous example that will be executed on the cluster:

.. code-block:: python

    import pandas as pd
    from pyspark.sql.functions import pandas_udf
    from pyspark.sql import SparkSession

    def main(spark):
        df = spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
            ("id", "v"))

        @pandas_udf("double")
        def mean_udf(v: pd.Series) -> float:
            return v.mean()

        print(df.groupby("id").agg(mean_udf(df['v'])).collect())


    if __name__ == "__main__":
        main(SparkSession.builder.getOrCreate())


There are multiple ways to manage Python dependencies in the cluster:

- Using PySpark Native Features
- Using Conda
- Using Virtualenv
- Using PEX


Using PySpark Native Features
-----------------------------

PySpark allows to upload Python files (``.py``), zipped Python packages (``.zip``), and Egg files (``.egg``)
to the executors by one of the following:

- Setting the configuration setting ``spark.submit.pyFiles``
- Setting ``--py-files`` option in Spark scripts
- Directly calling :meth:`pyspark.SparkContext.addPyFile` in applications

This is a straightforward method to ship additional custom Python code to the cluster. You can just add individual files or zip whole
packages and upload them. Using :meth:`pyspark.SparkContext.addPyFile` allows you to upload code even after having started your job.

However, it does not allow to add packages built as `Wheels <https://www.python.org/dev/peps/pep-0427/>`_ and therefore
does not allow to include dependencies with native code.


Using Conda
-----------

`Conda <https://docs.conda.io/en/latest/>`_ is one of the most widely-used Python package management systems. PySpark users can directly
use a Conda environment to ship their third-party Python packages by leveraging
`conda-pack <https://conda.github.io/conda-pack/spark.html>`_ which is a command line tool creating
relocatable Conda environments.

The example below creates a Conda environment to use on both the driver and executor and packs
it into an archive file. This archive file captures the Conda environment for Python and stores
both Python interpreter and all its relevant dependencies.

.. code-block:: bash

    conda create -y -n pyspark_conda_env -c conda-forge pyarrow pandas conda-pack
    conda activate pyspark_conda_env
    conda pack -f -o pyspark_conda_env.tar.gz

After that, you can ship it together with scripts or in the code by using the ``--archives`` option
or ``spark.archives`` configuration (``spark.yarn.dist.archives`` in YARN). It automatically unpacks the archive on executors.

In the case of a ``spark-submit`` script, you can use it as follows:

.. code-block:: bash

    export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
    export PYSPARK_PYTHON=./environment/bin/python
    spark-submit --archives pyspark_conda_env.tar.gz#environment app.py

Note that ``PYSPARK_DRIVER_PYTHON`` above should not be set for cluster modes in YARN or Kubernetes.

If you're on a regular Python shell or notebook, you can try it as shown below:

.. code-block:: python

    import os
    from pyspark.sql import SparkSession
    from app import main

    os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
    spark = SparkSession.builder.config(
        "spark.archives",  # 'spark.yarn.dist.archives' in YARN.
        "pyspark_conda_env.tar.gz#environment").getOrCreate()
    main(spark)

For a pyspark shell:

.. code-block:: bash

    export PYSPARK_DRIVER_PYTHON=python
    export PYSPARK_PYTHON=./environment/bin/python
    pyspark --archives pyspark_conda_env.tar.gz#environment


Using Virtualenv
----------------

`Virtualenv <https://virtualenv.pypa.io/en/latest/>`_  is a Python tool to create isolated Python environments.
Since Python 3.3, a subset of its features has been integrated into Python as a standard library under
the `venv <https://docs.python.org/3/library/venv.html>`_ module. PySpark users can use virtualenv to manage
Python dependencies in their clusters by using `venv-pack <https://jcristharif.com/venv-pack/index.html>`_
in a similar way as conda-pack.

A virtual environment to use on both driver and executor can be created as demonstrated below.
It packs the current virtual environment to an archive file, and it contains both Python interpreter and the dependencies.
However, it requires all nodes in a cluster to have the same Python interpreter installed because
`venv-pack packs Python interpreter as a symbolic link <https://github.com/jcrist/venv-pack/issues/5>`_.


.. code-block:: bash

    python -m venv pyspark_venv
    source pyspark_venv/bin/activate
    pip install pyarrow pandas venv-pack
    venv-pack -o pyspark_venv.tar.gz

You can directly pass/unpack the archive file and enable the environment on executors by leveraging
the ``--archives`` option or ``spark.archives`` configuration (``spark.yarn.dist.archives`` in YARN).

For ``spark-submit``, you can use it by running the command as follows. Also, notice that
``PYSPARK_DRIVER_PYTHON`` has to be unset in Kubernetes or YARN cluster modes.

.. code-block:: bash

    export PYSPARK_DRIVER_PYTHON=python # Do not set in cluster modes.
    export PYSPARK_PYTHON=./environment/bin/python
    spark-submit --archives pyspark_venv.tar.gz#environment app.py

For regular Python shells or notebooks:

.. code-block:: bash

    import os
    from pyspark.sql import SparkSession
    from app import main

    os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
    spark = SparkSession.builder.config(
        "spark.archives",  # 'spark.yarn.dist.archives' in YARN.
        "pyspark_venv.tar.gz#environment").getOrCreate()
    main(spark)

In the case of a pyspark shell:

.. code-block:: bash

    export PYSPARK_DRIVER_PYTHON=python
    export PYSPARK_PYTHON=./environment/bin/python
    pyspark --archives pyspark_venv.tar.gz#environment


Using PEX
---------

PySpark can also use `PEX <https://github.com/pantsbuild/pex>`_ to ship the Python packages
together. PEX is a tool that creates a self-contained Python environment. This is similar
to Conda or virtualenv, but a ``.pex`` file is executable by itself.

The following example creates a ``.pex`` file for the driver and executor to use.
The file contains the Python dependencies specified with the ``pex`` command.

.. code-block:: bash

    pip install pyarrow pandas pex
    pex pyspark pyarrow pandas -o pyspark_pex_env.pex

This file behaves similarly with a regular Python interpreter.

.. code-block:: bash

    ./pyspark_pex_env.pex -c "import pandas; print(pandas.__version__)"
    1.1.5

However, ``.pex`` file does not include a Python interpreter itself under the hood so all
nodes in a cluster should have the same Python interpreter installed.

In order to transfer and use the ``.pex`` file in a cluster, you should ship it via the
``spark.files`` configuration (``spark.yarn.dist.files`` in YARN) or ``--files`` option because they are regular files instead
of directories or archive files.

For application submission, you run the commands as shown below.
Note that ``PYSPARK_DRIVER_PYTHON`` should not be set for cluster modes in YARN or Kubernetes.

.. code-block:: bash

    export PYSPARK_DRIVER_PYTHON=python  # Do not set in cluster modes.
    export PYSPARK_PYTHON=./pyspark_pex_env.pex
    spark-submit --files pyspark_pex_env.pex app.py

For regular Python shells or notebooks:

.. code-block:: python

    import os
    from pyspark.sql import SparkSession
    from app import main

    os.environ['PYSPARK_PYTHON'] = "./pyspark_pex_env.pex"
    spark = SparkSession.builder.config(
        "spark.files",  # 'spark.yarn.dist.files' in YARN.
        "pyspark_pex_env.pex").getOrCreate()
    main(spark)

For the interactive pyspark shell, the commands are almost the same:

.. code-block:: bash

    export PYSPARK_DRIVER_PYTHON=python
    export PYSPARK_PYTHON=./pyspark_pex_env.pex
    pyspark --files pyspark_pex_env.pex

An end-to-end Docker example for deploying a standalone PySpark with ``SparkSession.builder`` and PEX
can be found `here <https://github.com/criteo/cluster-pack/blob/master/examples/spark-with-S3/README.md>`_
- it uses cluster-pack, a library on top of PEX that automatizes the intermediate step of having
to create & upload the PEX manually.
