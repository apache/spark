from shutil import rmtree
import tempfile
import numpy as np

from pyspark import SparkContext
from pyspark.ml import Estimator, Model, Pipeline, PipelineModel, Transformer
from pyspark.ml.classification import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import *
from pyspark.ml.feature import *
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.param.shared import HasMaxIter, HasInputCol, HasSeed
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.tuning import *
from pyspark.ml.util import keyword_only
from pyspark.ml.util import MLWritable, MLWriter
from pyspark.ml.wrapper import JavaParams
from pyspark.mllib.linalg import Vectors, DenseVector, SparseVector
from pyspark.sql import DataFrame, SQLContext, Row
from pyspark.sql.functions import rand

def _compare_pipelines(m1, m2):
    pass

sc = SparkContext(appName="test")
sc.setLogLevel("OFF")
sqlContext = SQLContext(sc)
temp_path = tempfile.mkdtemp()
try:
    df = sc.parallelize([
        Row(label=0.0, features=Vectors.dense(1.0, 0.8)),
        Row(label=0.0, features=Vectors.dense(0.8, 0.8)),
        Row(label=0.0, features=Vectors.dense(1.0, 1.2)),
        Row(label=1.0, features=Vectors.sparse(2, [], [])),
        Row(label=1.0, features=Vectors.sparse(2, [0], [0.1])),
        Row(label=1.0, features=Vectors.sparse(2, [1], [0.1])),
        Row(label=2.0, features=Vectors.dense(0.5, 0.5)),
        Row(label=2.0, features=Vectors.dense(0.3, 0.5)),
        Row(label=2.0, features=Vectors.dense(0.5, 0.6))]).toDF()
    lr = LogisticRegression()
    ovr = OneVsRest(classifier=lr)
    tvs_grid = ParamGridBuilder().addGrid(lr.maxIter, [1, 5]).build()
    tvs_evaluator = MulticlassClassificationEvaluator()
    tvs = TrainValidationSplit(estimator=ovr, estimatorParamMaps=tvs_grid,
                               evaluator=tvs_evaluator, trainRatio=1)
    cv_grid = ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).build()
    cv_evaluator = MulticlassClassificationEvaluator()
    cv = CrossValidator(estimator=tvs, estimatorParamMaps=cv_grid, evaluator=cv_evaluator,
                        numFolds=2)

    model = cv.fit(df)

    path = temp_path + "/nested-meta-algorithm"
    cv.save(path)
    loaded = CrossValidator.load(path)
    _compare_pipelines(cv, loaded)

    model_path = temp_path + "/nested-meta-model"
    model.save(model_path)
    loaded_model = CrossValidator.load(model_path)
    _compare_pipelines(model, loaded_model)
finally:
    try:
        rmtree(temp_path)
    except OSError:
        pass

sc.stop()
