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


======
Errors
======

Classes
-------

.. currentmodule:: pyspark.errors

.. autosummary::
    :toctree: api/

    AnalysisException
    ArithmeticException
    ArrayIndexOutOfBoundsException
    DateTimeException
    IllegalArgumentException
    NumberFormatException
    ParseException
    PySparkAssertionError
    PySparkAttributeError
    PySparkException
    PySparkKeyError
    PySparkNotImplementedError
    PySparkPicklingError
    PySparkRuntimeError
    PySparkTypeError
    PySparkValueError
    PySparkImportError
    PySparkIndexError
    PythonException
    QueryContext
    QueryContextType
    QueryExecutionException
    RetriesExceeded
    SessionNotSameException
    SparkRuntimeException
    SparkUpgradeException
    SparkNoSuchElementException
    StreamingQueryException
    TempTableAlreadyExistsException
    UnknownException
    UnsupportedOperationException


Methods
-------

.. currentmodule:: pyspark.errors

.. autosummary::
    :toctree: api/

    PySparkException.getCondition
    PySparkException.getErrorClass
    PySparkException.getMessage
    PySparkException.getMessageParameters
    PySparkException.getQueryContext
    PySparkException.getSqlState
