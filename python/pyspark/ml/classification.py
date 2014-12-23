from pyspark.sql import SchemaRDD
from pyspark.ml import _keyword_only_secret, _assert_keyword_only_args, _jvm
from pyspark.ml.param import Param


class LogisticRegression(object):
    """
    Logistic regression.
    """

    _java_class = "org.apache.spark.ml.classification.LogisticRegression"

    def __init__(self):
        self._java_obj = _jvm().org.apache.spark.ml.classification.LogisticRegression()
        self.paramMap = {}
        self.maxIter = Param(self, "maxIter", "max number of iterations", 100)
        self.regParam = Param(self, "regParam", "regularization constant", 0.1)

    def set(self, _keyword_only=_keyword_only_secret,
            maxIter=None, regParam=None):
        _assert_keyword_only_args()
        if maxIter is not None:
            self.paramMap[self.maxIter] = maxIter
        if regParam is not None:
            self.paramMap[self.regParam] = regParam
        return self

    # cannot chained
    def setMaxIter(self, value):
        self.paramMap[self.maxIter] = value
        return self

    def setRegParam(self, value):
        self.paramMap[self.regParam] = value
        return self

    def getMaxIter(self):
        if self.maxIter in self.paramMap:
            return self.paramMap[self.maxIter]
        else:
            return self.maxIter.defaultValue

    def getRegParam(self):
        if self.regParam in self.paramMap:
            return self.paramMap[self.regParam]
        else:
            return self.regParam.defaultValue

    def fit(self, dataset):
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

lr = LogisticRegression()

lr.set(maxIter=10, regParam=0.1)
