#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SchemaRDD, ArrayType, StringType, inherit_doc
from pyspark.ml import Transformer, _jvm
from pyspark.ml.param import Param

@inherit_doc
class Tokenizer(Transformer):

    def __init__(self):
        super(Tokenizer, self).__init__()
        self.inputCol = Param(self, "inputCol", "input column name", None)
        self.outputCol = Param(self, "outputCol", "output column name", None)
        self.paramMap = {}

    def setInputCol(self, value):
        self.paramMap[self.inputCol] = value
        return self

    def getInputCol(self):
        if self.inputCol in self.paramMap:
            return self.paramMap[self.inputCol]

    def setOutputCol(self, value):
        self.paramMap[self.outputCol] = value
        return self

    def getOutputCol(self):
        if self.outputCol in self.paramMap:
            return self.paramMap[self.outputCol]

    def transform(self, dataset, params={}):
        sqlCtx = dataset.sql_ctx
        if isinstance(params, dict):
            paramMap = self.paramMap.copy()
            paramMap.update(params)
            inputCol = paramMap[self.inputCol]
            outputCol = paramMap[self.outputCol]
            # TODO: make names unique
            sqlCtx.registerFunction("tokenize", lambda text: text.split(),
                                    ArrayType(StringType(), False))
            dataset.registerTempTable("dataset")
            return sqlCtx.sql("SELECT *, tokenize(%s) AS %s FROM dataset" % (inputCol, outputCol))
        elif isinstance(params, list):
            return [self.transform(dataset, paramMap) for paramMap in params]
        else:
            raise ValueError("The input params must be either a dict or a list.")


@inherit_doc
class HashingTF(Transformer):

    def __init__(self):
        super(HashingTF, self).__init__()
        self._java_obj = _jvm().org.apache.spark.ml.feature.HashingTF()
        self.numFeatures = Param(self, "numFeatures", "number of features", 1 << 18)
        self.inputCol = Param(self, "inputCol", "input column name")
        self.outputCol = Param(self, "outputCol", "output column name")

    def setNumFeatures(self, value):
        self._java_obj.setNumFeatures(value)
        return self

    def getNumFeatures(self):
        return self._java_obj.getNumFeatures()

    def setInputCol(self, value):
        self._java_obj.setInputCol(value)
        return self

    def getInputCol(self):
        return self._java_obj.getInputCol()

    def setOutputCol(self, value):
        self._java_obj.setOutputCol(value)
        return self

    def getOutputCol(self):
        return self._java_obj.getOutputCol()

    def transform(self, dataset, paramMap={}):
        if isinstance(paramMap, dict):
            javaParamMap = _jvm().org.apache.spark.ml.param.ParamMap()
            for k, v in paramMap.items():
                param = self._java_obj.getParam(k.name)
                javaParamMap.put(param, v)
            return SchemaRDD(self._java_obj.transform(dataset._jschema_rdd, javaParamMap),
                             dataset.sql_ctx)
        else:
            raise ValueError("paramMap must be a dict.")
