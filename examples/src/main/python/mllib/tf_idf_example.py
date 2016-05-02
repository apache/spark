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

from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.feature import HashingTF, IDF
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="TFIDFExample")  # SparkContext

    # $example on$
    # Load documents (one per line).
    documents = sc.textFile("data/mllib/kmeans_data.txt").map(lambda line: line.split(" "))

    hashingTF = HashingTF()
    tf = hashingTF.transform(documents)

    # While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    # First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)

    # spark.mllib's IDF implementation provides an option for ignoring terms
    # which occur in less than a minimum number of documents.
    # In such cases, the IDF for these terms is set to 0.
    # This feature can be used by passing the minDocFreq value to the IDF constructor.
    idfIgnore = IDF(minDocFreq=2).fit(tf)
    tfidfIgnore = idf.transform(tf)
    # $example off$

    print("tfidf:")
    for each in tfidf.collect():
        print(each)

    print("tfidfIgnore:")
    for each in tfidfIgnore.collect():
        print(each)

    sc.stop()
