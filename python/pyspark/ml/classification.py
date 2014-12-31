from pyspark.sql import SchemaRDD
from pyspark.ml import _jvm
from pyspark.ml.param import Param


class LogisticRegression(object):
    """
    Logistic regression.
    """

    # _java_class = "org.apache.spark.ml.classification.LogisticRegression"

    def __init__(self):
        self._java_obj = _jvm().org.apache.spark.ml.classification.LogisticRegression()
        self.maxIter = Param(self, "maxIter", "max number of iterations", 100)
        self.regParam = Param(self, "regParam", "regularization constant", 0.1)
        self.featuresCol = Param(self, "featuresCol", "features column name", "features")

    def setMaxIter(self, value):
        self._java_obj.setMaxIter(value)
        return self

    def getMaxIter(self):
        return self._java_obj.getMaxIter()

    def setRegParam(self, value):
        self._java_obj.setRegParam(value)
        return self

    def getRegParam(self):
        return self._java_obj.getRegParam()

    def setFeaturesCol(self, value):
        self._java_obj.setFeaturesCol(value)
        return self

    def getFeaturesCol(self):
        return self._java_obj.getFeaturesCol()

    def fit(self, dataset, params=None):
        """
        Fits a dataset with optional parameters.
        """
        java_model = self._java_obj.fit(dataset._jschema_rdd, _jvm().org.apache.spark.ml.param.ParamMap())
        return LogisticRegressionModel(java_model)


class LogisticRegressionModel(object):
    """
    Model fitted by LogisticRegression.
    """

    def __init__(self, _java_model):
        self._java_model = _java_model

    def transform(self, dataset):
        return SchemaRDD(self._java_model.transform(dataset._jschema_rdd, _jvm().org.apache.spark.ml.param.ParamMap()), dataset.sql_ctx)

