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


MLlib (DataFrame-based) for Spark Connect
=========================================

.. warning::
    The namespace for this package can change in the future Spark version.


Pipeline APIs
-------------

.. currentmodule:: pyspark.ml.connect

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Transformer
    Estimator
    Model
    Evaluator
    Pipeline
    PipelineModel


Feature
-------

.. currentmodule:: pyspark.ml.connect.feature

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    MaxAbsScaler
    MaxAbsScalerModel
    StandardScaler
    StandardScalerModel
    ArrayAssembler


Classification
--------------

.. currentmodule:: pyspark.ml.connect.classification

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    LogisticRegression
    LogisticRegressionModel


Functions
---------

.. currentmodule:: pyspark.ml.connect.functions

.. autosummary::
    :toctree: api/

    array_to_vector
    vector_to_array


Tuning
------

.. currentmodule:: pyspark.ml.connect.tuning

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    CrossValidator
    CrossValidatorModel


Evaluation
----------

.. currentmodule:: pyspark.ml.connect.evaluation

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    RegressionEvaluator
    BinaryClassificationEvaluator
    MulticlassClassificationEvaluator


Utilities
---------

.. currentmodule:: pyspark.ml.connect.io_utils

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    ParamsReadWrite
    CoreModelReadWrite
    MetaAlgorithmReadWrite

