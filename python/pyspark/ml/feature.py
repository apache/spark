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

from pyspark.ml.param.shared import HasInputCol, HasOutputCol, HasNumFeatures
from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaTransformer
from pyspark.mllib.common import inherit_doc

__all__ = ['Tokenizer', 'HashingTF']


@inherit_doc
class Tokenizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A tokenizer that converts the input string to lowercase and then
    splits it by white spaces.

    >>> from pyspark.sql import Row
    >>> df = sc.parallelize([Row(text="a b c")]).toDF()
    >>> tokenizer = Tokenizer(inputCol="text", outputCol="words")
    >>> print tokenizer.transform(df).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> # Change a parameter.
    >>> print tokenizer.setParams(outputCol="tokens").transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Temporarily modify a parameter.
    >>> print tokenizer.transform(df, {tokenizer.outputCol: "words"}).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> print tokenizer.transform(df).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    >>> # Must use keyword arguments to specify params.
    >>> tokenizer.setParams("text")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    """

    _java_class = "org.apache.spark.ml.feature.Tokenizer"

    @keyword_only
    def __init__(self, inputCol="input", outputCol="output"):
        """
        __init__(self, inputCol="input", outputCol="output")
        """
        super(Tokenizer, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol="input", outputCol="output"):
        """
        setParams(self, inputCol="input", outputCol="output")
        Sets params for this Tokenizer.
        """
        kwargs = self.setParams._input_kwargs
        return self._set_params(**kwargs)


@inherit_doc
class HashingTF(JavaTransformer, HasInputCol, HasOutputCol, HasNumFeatures):
    """
    Maps a sequence of terms to their term frequencies using the
    hashing trick.

    >>> from pyspark.sql import Row
    >>> df = sc.parallelize([Row(words=["a", "b", "c"])]).toDF()
    >>> hashingTF = HashingTF(numFeatures=10, inputCol="words", outputCol="features")
    >>> print hashingTF.transform(df).head().features
    (10,[7,8,9],[1.0,1.0,1.0])
    >>> print hashingTF.setParams(outputCol="freqs").transform(df).head().freqs
    (10,[7,8,9],[1.0,1.0,1.0])
    >>> params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}
    >>> print hashingTF.transform(df, params).head().vector
    (5,[2,3,4],[1.0,1.0,1.0])
    """

    _java_class = "org.apache.spark.ml.feature.HashingTF"

    @keyword_only
    def __init__(self, numFeatures=1 << 18, inputCol="input", outputCol="output"):
        """
        __init__(self, numFeatures=1 << 18, inputCol="input", outputCol="output")
        """
        super(HashingTF, self).__init__()
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, numFeatures=1 << 18, inputCol="input", outputCol="output"):
        """
        setParams(self, numFeatures=1 << 18, inputCol="input", outputCol="output")
        Sets params for this HashingTF.
        """
        kwargs = self.setParams._input_kwargs
        return self._set_params(**kwargs)


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.feature tests")
    sqlCtx = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlCtx'] = sqlCtx
    (failure_count, test_count) = doctest.testmod(
        globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
