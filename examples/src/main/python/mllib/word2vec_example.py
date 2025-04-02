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

from pyspark import SparkContext
# $example on$
from pyspark.mllib.feature import Word2Vec
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="Word2VecExample")  # SparkContext

    # $example on$
    inp = sc.textFile("data/mllib/sample_lda_data.txt").map(lambda row: row.split(" "))

    word2vec = Word2Vec()
    model = word2vec.fit(inp)

    synonyms = model.findSynonyms('1', 5)

    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))
    # $example off$

    sc.stop()
