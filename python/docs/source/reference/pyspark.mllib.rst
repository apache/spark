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


MLlib (RDD-based)
=================

Classification
--------------

.. currentmodule:: pyspark.mllib.classification

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    LogisticRegressionModel
    LogisticRegressionWithSGD
    LogisticRegressionWithLBFGS
    SVMModel
    SVMWithSGD
    NaiveBayesModel
    NaiveBayes
    StreamingLogisticRegressionWithSGD


Clustering
----------

.. currentmodule:: pyspark.mllib.clustering

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/


    BisectingKMeansModel
    BisectingKMeans
    KMeansModel
    KMeans
    GaussianMixtureModel
    GaussianMixture
    PowerIterationClusteringModel
    PowerIterationClustering
    StreamingKMeans
    StreamingKMeansModel
    LDA
    LDAModel


Evaluation
----------

.. currentmodule:: pyspark.mllib.evaluation

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    BinaryClassificationMetrics
    RegressionMetrics
    MulticlassMetrics
    RankingMetrics


Feature
-------

.. currentmodule:: pyspark.mllib.feature

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Normalizer
    StandardScalerModel
    StandardScaler
    HashingTF
    IDFModel
    IDF
    Word2Vec
    Word2VecModel
    ChiSqSelector
    ChiSqSelectorModel
    ElementwiseProduct


Frequency Pattern Mining
------------------------

.. currentmodule:: pyspark.mllib.fpm

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    FPGrowth
    FPGrowthModel
    PrefixSpan
    PrefixSpanModel


Vector and Matrix
-----------------

.. currentmodule:: pyspark.mllib.linalg

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Vector
    DenseVector
    SparseVector
    Vectors
    Matrix
    DenseMatrix
    SparseMatrix
    Matrices
    QRDecomposition


Distributed Representation
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: pyspark.mllib.linalg.distributed

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    BlockMatrix
    CoordinateMatrix
    DistributedMatrix
    IndexedRow
    IndexedRowMatrix
    MatrixEntry
    RowMatrix
    SingularValueDecomposition


Random
------

.. currentmodule:: pyspark.mllib.random

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    RandomRDDs


Recommendation
--------------

.. currentmodule:: pyspark.mllib.recommendation

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    MatrixFactorizationModel
    ALS
    Rating


Regression
----------

.. currentmodule:: pyspark.mllib.regression

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    LabeledPoint
    LinearModel
    LinearRegressionModel
    LinearRegressionWithSGD
    RidgeRegressionModel
    RidgeRegressionWithSGD
    LassoModel
    LassoWithSGD
    IsotonicRegressionModel
    IsotonicRegression
    StreamingLinearAlgorithm
    StreamingLinearRegressionWithSGD


Statistics
----------

.. currentmodule:: pyspark.mllib.stat

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Statistics
    MultivariateStatisticalSummary
    ChiSqTestResult
    MultivariateGaussian
    KernelDensity
    ChiSqTestResult
    KolmogorovSmirnovTestResult


Tree
----

.. currentmodule:: pyspark.mllib.tree

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    DecisionTreeModel
    DecisionTree
    RandomForestModel
    RandomForest
    GradientBoostedTreesModel
    GradientBoostedTrees


Utilities
---------

.. currentmodule:: pyspark.mllib.util

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    JavaLoader
    JavaSaveable
    LinearDataGenerator
    Loader
    MLUtils
    Saveable
