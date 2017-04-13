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

""" An example of Latent Dirichlet Allocation (LDA) with PySpark. """

from __future__ import print_function

from pyspark import SparkContext
from pyspark.mllib.clustering import LDA
from pyspark.mllib.linalg import Vectors


def parse_line(line):
    return Vectors.dense([float(x) for x in line.split(' ')])


if __name__ == "__main__":

    sc = SparkContext(appName="lda_example")

    term_count_rdd = sc.textFile("data/mllib/sample_lda_data.txt").map(parse_line)

    # prepare the corpus as an RDD of [document_id, vector_of_term_counts] lists
    corpus = term_count_rdd.zipWithIndex().map(lambda (term_counts, doc_id): [doc_id, term_counts])

    # get a summary of the corpus
    corpus.cache()
    actualCorpusSize = corpus.count()
    actualVocabSize = len(corpus.first()[1])
    actualTotalTokens = corpus.values().map(lambda term_counts: term_counts.sum()).sum()

    print("Corpus summary:")
    print("\t Training set size: %d documents" % actualCorpusSize)
    print("\t Vocabulary size: %d terms" % actualVocabSize)
    print("\t Number of term counts: %d tokens" % actualTotalTokens)

    model = LDA.train(corpus, k=10, maxIterations=10, optimizer="online")

    topics = model.describeTopics(3)

    print("\"topic\", \"termIndices\", \"termWeights\"")
    for i, t in enumerate(topics):
        print("%d, %s, %s" % (i, str(t[0]), str(t[1])))

    sc.stop()
