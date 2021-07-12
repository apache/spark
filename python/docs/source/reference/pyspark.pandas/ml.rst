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
