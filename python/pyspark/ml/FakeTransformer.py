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

import sys
if sys.version > '3':
    basestring = str

from pyspark import since, keyword_only
from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml import Transformer
from pyspark.ml.linalg import _convert_to_vector
from pyspark.ml.param.shared import *
from pyspark.ml.util import JavaMLReadable, JavaMLWritable
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaTransformer, _jvm
from pyspark.mllib.common import inherit_doc
from pyspark.sql.functions import concat, col, lit
from pyspark.sql import DataFrame
from pyspark.ml.util import TransformerWrapper

__all__ = ['Int2Str', 'Int2StrJVM']


@inherit_doc
class Int2Str(Transformer, HasInputCol, HasOutputCol):
    suffix = Param(Params._dummy(), "suffix",
                   "Suffix adds to an Integer",
                   typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, suffix="", inputCol=None, outputCol=None):
        """
        __init__(self, suffix="", inputCol=None, outputCol=None)
        """
        super(Int2Str, self).__init__()
        self._setDefault(suffix="")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, suffix="", inputCol=None, outputCol=None):
        """
        setParams(self, suffix=0.0, inputCol=None, outputCol=None)
        Sets params for this Binarizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.4.0")
    def setSuffix(self, value):
        """
        Sets the value of :py:attr:`threshold`.
        """
        return self._set(suffix=value)

    @since("1.4.0")
    def getSuffix(self):
        """
        Gets the value of threshold or its default value.
        """
        return self.getOrDefault(self.suffix)

    def _transform(self, dataset):
        inc = self.getInputCol()
        ouc = self.getOutputCol()
        suff2 = dataset.count()
        suff = self.getSuffix()
        return dataset.withColumn(ouc, concat(dataset[inc], lit(suff), lit(str(suff2))))


class Int2StrJVM(object):
    def __init__(self, suffix, inputCol, outputCol, df):
        int2str = Int2Str(suffix=suffix, inputCol=inputCol, outputCol=outputCol)
        wrapper = TransformerWrapper(df.sql_ctx, int2str)
        self.jtransformer = _jvm().org.apache.spark.ml.api.python.PythonTransformer(wrapper)
        self.df = df

    def transform(self):
        jdf = self.jtransformer.transform(self.df._jdf)
        return DataFrame(jdf, self.df.sql_ctx)