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
from pyspark.ml.util import inherit_doc
from pyspark.ml.wrapper import JavaTransformer

__all__ = ['Tokenizer', 'HashingTF']


@inherit_doc
class Tokenizer(JavaTransformer, HasInputCol, HasOutputCol):
    """
    A tokenizer that converts the input string to lowercase and then
    splits it by white spaces.

    >>> from pyspark.sql import Row
    >>> dataset = sqlCtx.inferSchema(sc.parallelize([Row(text="a b c")]))
    >>> tokenizer = Tokenizer() \
            .setInputCol("text") \
            .setOutputCol("words")
    >>> print tokenizer.transform(dataset).head()
    Row(text=u'a b c', words=[u'a', u'b', u'c'])
    >>> print tokenizer.transform(dataset, {tokenizer.outputCol: "tokens"}).head()
    Row(text=u'a b c', tokens=[u'a', u'b', u'c'])
    """

    _java_class = "org.apache.spark.ml.feature.Tokenizer"


@inherit_doc
class HashingTF(JavaTransformer, HasInputCol, HasOutputCol, HasNumFeatures):
    """
    Maps a sequence of terms to their term frequencies using the
    hashing trick.

    >>> from pyspark.sql import Row
    >>> dataset = sqlCtx.inferSchema(sc.parallelize([Row(words=["a", "b", "c"])]))
    >>> hashingTF = HashingTF() \
            .setNumFeatures(10) \
            .setInputCol("words") \
            .setOutputCol("features")
    >>> print hashingTF.transform(dataset).head().features
    (10,[7,8,9],[1.0,1.0,1.0])
    >>> params = {hashingTF.numFeatures: 5, hashingTF.outputCol: "vector"}
    >>> print hashingTF.transform(dataset, params).head().vector
    (5,[2,3,4],[1.0,1.0,1.0])
    """

    _java_class = "org.apache.spark.ml.feature.HashingTF"


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
