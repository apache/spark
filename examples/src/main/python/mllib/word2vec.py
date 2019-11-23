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

# This example uses text8 file from http://mattmahoney.net/dc/text8.zip
# The file was downloaded, unzipped and split into multiple lines using
#
# wget http://mattmahoney.net/dc/text8.zip
# unzip text8.zip
# grep -o -E '\w+(\W+\w+){0,15}' text8 > text8_lines
# This was done so that the example can be run in local mode

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

USAGE = ("bin/spark-submit --driver-memory 4g "
         "examples/src/main/python/mllib/word2vec.py text8_lines")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit("Argument for file not provided")
    file_path = sys.argv[1]
    sc = SparkContext(appName='Word2Vec')
    inp = sc.textFile(file_path).map(lambda row: row.split(" "))

    word2vec = Word2Vec()
    model = word2vec.fit(inp)

    synonyms = model.findSynonyms('china', 40)

    for word, cosine_distance in synonyms:
        print("{}: {}".format(word, cosine_distance))
    sc.stop()
