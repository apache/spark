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

from pyspark.rdd import ignore_unicode_prefix
from pyspark.ml.param.shared import HasInputCol, HasInputCols, HasOutputCol, HasNumFeatures
from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaTransformer
from pyspark.mllib.common import inherit_doc

__all__ = ['Tokenizer', 'HashingTF', 'VectorAssembler']


@inherit_doc
@ignore_unicode_prefix
class Tokenizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A tokenizer that converts the input string to lowercase and then
    splits it by white spaces.

    >>> from pyspark.sql import Row
    >>> df = sc.parallelize([Row(text="a b c")]).toDF()
    >>> tokenizer = Tokenizer(inputCol="text", outputCol="words")
    >>> tokenizer.transform(df).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> # Change a parameter.
    >>> tokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Temporarily modify a parameter.
    >>> tokenizer.transform(df, {tokenizer.outputCol: "words"}).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> tokenizer.transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Must use keyword arguments to specify params.
    >>> tokenizer.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """

    _java_class = "org.apache.spark.ml.feature.Tokenizer"

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        """
        __init__(self, inputCol=None, outputCol=None)
        """
        super(Tokenizer, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        """
        setParams(self, inputCol="input", outputCol="output")
        Sets params for this Tokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class HashingTF(JavaTransformer, HasInputCol, HasOutputCol, HasNumFeatures):
    """
    Maps a sequence of terms to their term frequencies using the
    hashing trick.

    >>> from pyspark.sql import Row
    >>> df = sc.parallelize([Row(words=["a", "b", "c"])]).toDF()
    >>> hashingTF = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
    >>> hashingTF.transform(df).head().features
    SparseVector(10, {7: 1.0, 8: 1.0, 9: 1.0})
    >>> hashingTF.setParams(outputCol="freqs").transform(df).head().freqs
    SparseVector(10, {7: 1.0, 8: 1.0, 9: 1.0})
    >>> params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}
    >>> hashingTF.transform(df, params).head().vector
    SparseVector(5, {2: 1.0, 3: 1.0, 4: 1.0})
    """

    _java_class = "org.apache.spark.ml.feature.HashingTF"

    @keyword_only
    def __init__(self, numFeatures=1 << 18, inputCol=None, outputCol=None):
        """
        __init__(self, numFeatures=1 << 18, inputCol=None, outputCol=None)
        """
        super(HashingTF, self).__init__()
        self._setDefault(numFeatures=1 << 18)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, numFeatures=1 << 18, inputCol=None, outputCol=None):
        """
        setParams(self, numFeatures=1 << 18, inputCol=None, outputCol=None)
        Sets params for this HashingTF.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


@inherit_doc
class VectorAssembler(JavaTransformer, HasInputCols, HasOutputCol):
    """
    A feature transformer that merges multiple columns into a vector column.

    >>> from pyspark.sql import Row
    >>> df = sc.parallelize([Row(a=1, b=0, c=3)]).toDF()
    >>> vecAssembler = VectorAssembler(inputCols=["a", "b", "c"], outputCol="features")
    >>> vecAssembler.transform(df).head().features
    SparseVector(3, {0: 1.0, 2: 3.0})
    >>> vecAssembler.setParams(outputCol="freqs").transform(df).head().freqs
    SparseVector(3, {0: 1.0, 2: 3.0})
    >>> params = {vecAssembler.inputCols: ["b", "a"], vecAssembler.outputCol: "vector"}
    >>> vecAssembler.transform(df, params).head().vector
    SparseVector(2, {1: 1.0})
    """

    _java_class = "org.apache.spark.ml.feature.VectorAssembler"

    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        """
        __init__(self, inputCols=None, outputCol=None)
        """
        super(VectorAssembler, self).__init__()
        self._setDefault()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        """
        setParams(self, inputCols=None, outputCol=None)
        Sets params for this VectorAssembler.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.feature tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
