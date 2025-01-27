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


MLlib (DataFrame-based)
=======================

Pipeline APIs
-------------

.. currentmodule:: pyspark.ml

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Transformer
    UnaryTransformer
    Estimator
    Model
    Predictor
    PredictionModel
    Pipeline
    PipelineModel


Parameters
----------

.. currentmodule:: pyspark.ml.param

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Param
    Params
    TypeConverters


Feature
-------

.. currentmodule:: pyspark.ml.feature

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Binarizer
    BucketedRandomProjectionLSH
    BucketedRandomProjectionLSHModel
    Bucketizer
    ChiSqSelector
    ChiSqSelectorModel
    CountVectorizer
    CountVectorizerModel
    DCT
    ElementwiseProduct
    FeatureHasher
    HashingTF
    IDF
    IDFModel
    Imputer
    ImputerModel
    IndexToString
    Interaction
    MaxAbsScaler
    MaxAbsScalerModel
    MinHashLSH
    MinHashLSHModel
    MinMaxScaler
    MinMaxScalerModel
    NGram
    Normalizer
    OneHotEncoder
    OneHotEncoderModel
    PCA
    PCAModel
    PolynomialExpansion
    QuantileDiscretizer
    RobustScaler
    RobustScalerModel
    RegexTokenizer
    RFormula
    RFormulaModel
    SQLTransformer
    StandardScaler
    StandardScalerModel
    StopWordsRemover
    StringIndexer
    StringIndexerModel
    TargetEncoder
    TargetEncoderModel
    Tokenizer
    UnivariateFeatureSelector
    UnivariateFeatureSelectorModel
    VarianceThresholdSelector
    VarianceThresholdSelectorModel
    VectorAssembler
    VectorIndexer
    VectorIndexerModel
    VectorSizeHint
    VectorSlicer
    Word2Vec
    Word2VecModel


Classification
--------------

.. currentmodule:: pyspark.ml.classification

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    LinearSVC
    LinearSVCModel
    LinearSVCSummary
    LinearSVCTrainingSummary
    LogisticRegression
    LogisticRegressionModel
    LogisticRegressionSummary
    LogisticRegressionTrainingSummary
    BinaryLogisticRegressionSummary
    BinaryLogisticRegressionTrainingSummary
    DecisionTreeClassifier
    DecisionTreeClassificationModel
    GBTClassifier
    GBTClassificationModel
    RandomForestClassifier
    RandomForestClassificationModel
    RandomForestClassificationSummary
    RandomForestClassificationTrainingSummary
    BinaryRandomForestClassificationSummary
    BinaryRandomForestClassificationTrainingSummary
    NaiveBayes
    NaiveBayesModel
    MultilayerPerceptronClassifier
    MultilayerPerceptronClassificationModel
    MultilayerPerceptronClassificationSummary
    MultilayerPerceptronClassificationTrainingSummary
    OneVsRest
    OneVsRestModel
    FMClassifier
    FMClassificationModel
    FMClassificationSummary
    FMClassificationTrainingSummary


Clustering
----------

.. currentmodule:: pyspark.ml.clustering

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    BisectingKMeans
    BisectingKMeansModel
    BisectingKMeansSummary
    KMeans
    KMeansModel
    KMeansSummary
    GaussianMixture
    GaussianMixtureModel
    GaussianMixtureSummary
    LDA
    LDAModel
    LocalLDAModel
    DistributedLDAModel
    PowerIterationClustering


Functions
---------

.. currentmodule:: pyspark.ml.functions

.. autosummary::
    :toctree: api/

    array_to_vector
    vector_to_array
    predict_batch_udf


Vector and Matrix
-----------------

.. currentmodule:: pyspark.ml.linalg

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


Recommendation
--------------

.. currentmodule:: pyspark.ml.recommendation

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    ALS
    ALSModel


Regression
----------

.. currentmodule:: pyspark.ml.regression

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    AFTSurvivalRegression
    AFTSurvivalRegressionModel
    DecisionTreeRegressor
    DecisionTreeRegressionModel
    GBTRegressor
    GBTRegressionModel
    GeneralizedLinearRegression
    GeneralizedLinearRegressionModel
    GeneralizedLinearRegressionSummary
    GeneralizedLinearRegressionTrainingSummary
    IsotonicRegression
    IsotonicRegressionModel
    LinearRegression
    LinearRegressionModel
    LinearRegressionSummary
    LinearRegressionTrainingSummary
    RandomForestRegressor
    RandomForestRegressionModel
    FMRegressor
    FMRegressionModel


Statistics
----------

.. currentmodule:: pyspark.ml.stat

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    ChiSquareTest
    Correlation
    KolmogorovSmirnovTest
    MultivariateGaussian
    Summarizer
    SummaryBuilder


Tuning
------

.. currentmodule:: pyspark.ml.tuning

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    ParamGridBuilder
    CrossValidator
    CrossValidatorModel
    TrainValidationSplit
    TrainValidationSplitModel


Evaluation
----------

.. currentmodule:: pyspark.ml.evaluation

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    Evaluator
    BinaryClassificationEvaluator
    RegressionEvaluator
    MulticlassClassificationEvaluator
    MultilabelClassificationEvaluator
    ClusteringEvaluator
    RankingEvaluator


Frequency Pattern Mining
----------------------------

.. currentmodule:: pyspark.ml.fpm

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    FPGrowth
    FPGrowthModel
    PrefixSpan


Image
-----

.. currentmodule:: pyspark.ml.image

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    ImageSchema
    _ImageSchema


Distributor
-----------


.. currentmodule:: pyspark.ml.torch.distributor

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    TorchDistributor

.. currentmodule:: pyspark.ml.deepspeed.deepspeed_distributor
.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    DeepspeedTorchDistributor


Utilities
---------

.. currentmodule:: pyspark.ml.util

.. autosummary::
    :template: autosummary/class_with_docs.rst
    :toctree: api/

    BaseReadWrite
    DefaultParamsReadable
    DefaultParamsReader
    DefaultParamsWritable
    DefaultParamsWriter
    GeneralMLWriter
    HasTrainingSummary
    Identifiable
    MLReadable
    MLReader
    MLWritable
    MLWriter

