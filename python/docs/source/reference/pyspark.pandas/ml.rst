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


.. _api.ml:

==========================
Machine Learning utilities
==========================
.. currentmodule:: pyspark.pandas.mlflow

MLflow
------

Arbitrary MLflow models can be used with pandas-on-Spark Dataframes,
provided they implement the 'pyfunc' flavor. This is the case
for most frameworks supported by MLflow (scikit-learn, pytorch,
tensorflow, ...). See comprehensive examples in
:func:`load_model` for more information.

.. note::
   The MLflow package must be installed in order to use this module.

.. autosummary::
   :toctree: api/

   PythonModelWrapper
   load_model
