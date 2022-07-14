# -*- coding: utf-8 -*-
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

import unittest

from pyspark.ml.feature import (
    Binarizer,
    CountVectorizer,
    CountVectorizerModel,
    HashingTF,
    IDF,
    NGram,
    RFormula,
    StopWordsRemover,
    StringIndexer,
    StringIndexerModel,
    VectorSizeHint,
)
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors
from pyspark.sql import Row
from pyspark.testing.utils import QuietTest
from pyspark.testing.mlutils import check_params, SparkSessionTestCase


class FeatureTests(SparkSessionTestCase):
    def test_binarizer(self):
        b0 = Binarizer()
        self.assertListEqual(
            b0.params,
            [b0.inputCol, b0.inputCols, b0.outputCol, b0.outputCols, b0.threshold, b0.thresholds],
        )
        self.assertTrue(all([~b0.isSet(p) for p in b0.params]))
        self.assertTrue(b0.hasDefault(b0.threshold))
        self.assertEqual(b0.getThreshold(), 0.0)
        b0.setParams(inputCol="input", outputCol="output").setThreshold(1.0)
        self.assertTrue(not all([b0.isSet(p) for p in b0.params]))
        self.assertEqual(b0.getThreshold(), 1.0)
        self.assertEqual(b0.getInputCol(), "input")
        self.assertEqual(b0.getOutputCol(), "output")

        b0c = b0.copy({b0.threshold: 2.0})
        self.assertEqual(b0c.uid, b0.uid)
        self.assertListEqual(b0c.params, b0.params)
        self.assertEqual(b0c.getThreshold(), 2.0)

        b1 = Binarizer(threshold=2.0, inputCol="input", outputCol="output")
        self.assertNotEqual(b1.uid, b0.uid)
        self.assertEqual(b1.getThreshold(), 2.0)
        self.assertEqual(b1.getInputCol(), "input")
        self.assertEqual(b1.getOutputCol(), "output")

    def test_idf(self):
        dataset = self.spark.createDataFrame(
            [(DenseVector([1.0, 2.0]),), (DenseVector([0.0, 1.0]),), (DenseVector([3.0, 0.2]),)],
            ["tf"],
        )
        idf0 = IDF(inputCol="tf")
        self.assertListEqual(idf0.params, [idf0.inputCol, idf0.minDocFreq, idf0.outputCol])
        idf0m = idf0.fit(dataset, {idf0.outputCol: "idf"})
        self.assertEqual(
            idf0m.uid, idf0.uid, "Model should inherit the UID from its parent estimator."
        )
        output = idf0m.transform(dataset)
        self.assertIsNotNone(output.head().idf)
        self.assertIsNotNone(idf0m.docFreq)
        self.assertEqual(idf0m.numDocs, 3)
        # Test that parameters transferred to Python Model
        check_params(self, idf0m)

    def test_ngram(self):
        dataset = self.spark.createDataFrame([Row(input=["a", "b", "c", "d", "e"])])
        ngram0 = NGram(n=4, inputCol="input", outputCol="output")
        self.assertEqual(ngram0.getN(), 4)
        self.assertEqual(ngram0.getInputCol(), "input")
        self.assertEqual(ngram0.getOutputCol(), "output")
        transformedDF = ngram0.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["a b c d", "b c d e"])

    def test_stopwordsremover(self):
        dataset = self.spark.createDataFrame([Row(input=["a", "panda"])])
        stopWordRemover = StopWordsRemover(inputCol="input", outputCol="output")
        # Default
        self.assertEqual(stopWordRemover.getInputCol(), "input")
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["panda"])
        self.assertEqual(type(stopWordRemover.getStopWords()), list)
        self.assertTrue(isinstance(stopWordRemover.getStopWords()[0], str))
        # Custom
        stopwords = ["panda"]
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getInputCol(), "input")
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, ["a"])
        # with language selection
        stopwords = StopWordsRemover.loadDefaultStopWords("turkish")
        dataset = self.spark.createDataFrame([Row(input=["acaba", "ama", "biri"])])
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, [])
        # with locale
        stopwords = ["BELKİ"]
        dataset = self.spark.createDataFrame([Row(input=["belki"])])
        stopWordRemover.setStopWords(stopwords).setLocale("tr")
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, [])

    def test_count_vectorizer_with_binary(self):
        dataset = self.spark.createDataFrame(
            [
                (
                    0,
                    "a a a b b c".split(" "),
                    SparseVector(3, {0: 1.0, 1: 1.0, 2: 1.0}),
                ),
                (
                    1,
                    "a a".split(" "),
                    SparseVector(3, {0: 1.0}),
                ),
                (
                    2,
                    "a b".split(" "),
                    SparseVector(3, {0: 1.0, 1: 1.0}),
                ),
                (
                    3,
                    "c".split(" "),
                    SparseVector(3, {2: 1.0}),
                ),
            ],
            ["id", "words", "expected"],
        )
        cv = CountVectorizer(binary=True, inputCol="words", outputCol="features")
        model = cv.fit(dataset)

        transformedList = model.transform(dataset).select("features", "expected").collect()

        for r in transformedList:
            feature, expected = r
            self.assertEqual(feature, expected)

    def test_count_vectorizer_with_maxDF(self):
        dataset = self.spark.createDataFrame(
            [
                (
                    0,
                    "a b c d".split(" "),
                    SparseVector(3, {0: 1.0, 1: 1.0, 2: 1.0}),
                ),
                (
                    1,
                    "a b c".split(" "),
                    SparseVector(3, {0: 1.0, 1: 1.0}),
                ),
                (
                    2,
                    "a b".split(" "),
                    SparseVector(3, {0: 1.0}),
                ),
                (
                    3,
                    "a".split(" "),
                    SparseVector(3, {}),
                ),
            ],
            ["id", "words", "expected"],
        )
        cv = CountVectorizer(inputCol="words", outputCol="features")
        model1 = cv.setMaxDF(3).fit(dataset)
        self.assertEqual(model1.vocabulary, ["b", "c", "d"])

        transformedList1 = model1.transform(dataset).select("features", "expected").collect()

        for r in transformedList1:
            feature, expected = r
            self.assertEqual(feature, expected)

        model2 = cv.setMaxDF(0.75).fit(dataset)
        self.assertEqual(model2.vocabulary, ["b", "c", "d"])

        transformedList2 = model2.transform(dataset).select("features", "expected").collect()

        for r in transformedList2:
            feature, expected = r
            self.assertEqual(feature, expected)

    def test_count_vectorizer_from_vocab(self):
        model = CountVectorizerModel.from_vocabulary(
            ["a", "b", "c"], inputCol="words", outputCol="features", minTF=2
        )
        self.assertEqual(model.vocabulary, ["a", "b", "c"])
        self.assertEqual(model.getMinTF(), 2)

        dataset = self.spark.createDataFrame(
            [
                (
                    0,
                    "a a a b b c".split(" "),
                    SparseVector(3, {0: 3.0, 1: 2.0}),
                ),
                (
                    1,
                    "a a".split(" "),
                    SparseVector(3, {0: 2.0}),
                ),
                (
                    2,
                    "a b".split(" "),
                    SparseVector(3, {}),
                ),
            ],
            ["id", "words", "expected"],
        )

        transformed_list = model.transform(dataset).select("features", "expected").collect()

        for r in transformed_list:
            feature, expected = r
            self.assertEqual(feature, expected)

        # Test an empty vocabulary
        with QuietTest(self.sc):
            with self.assertRaisesRegex(Exception, "vocabSize.*invalid.*0"):
                CountVectorizerModel.from_vocabulary([], inputCol="words")

        # Test model with default settings can transform
        model_default = CountVectorizerModel.from_vocabulary(["a", "b", "c"], inputCol="words")
        transformed_list = (
            model_default.transform(dataset)
            .select(model_default.getOrDefault(model_default.outputCol))
            .collect()
        )
        self.assertEqual(len(transformed_list), 3)

    def test_rformula_force_index_label(self):
        df = self.spark.createDataFrame(
            [(1.0, 1.0, "a"), (0.0, 2.0, "b"), (1.0, 0.0, "a")], ["y", "x", "s"]
        )
        # Does not index label by default since it's numeric type.
        rf = RFormula(formula="y ~ x + s")
        model = rf.fit(df)
        transformedDF = model.transform(df)
        self.assertEqual(transformedDF.head().label, 1.0)
        # Force to index label.
        rf2 = RFormula(formula="y ~ x + s").setForceIndexLabel(True)
        model2 = rf2.fit(df)
        transformedDF2 = model2.transform(df)
        self.assertEqual(transformedDF2.head().label, 0.0)

    def test_rformula_string_indexer_order_type(self):
        df = self.spark.createDataFrame(
            [(1.0, 1.0, "a"), (0.0, 2.0, "b"), (1.0, 0.0, "a")], ["y", "x", "s"]
        )
        rf = RFormula(formula="y ~ x + s", stringIndexerOrderType="alphabetDesc")
        self.assertEqual(rf.getStringIndexerOrderType(), "alphabetDesc")
        transformedDF = rf.fit(df).transform(df)
        observed = transformedDF.select("features").collect()
        expected = [[1.0, 0.0], [2.0, 1.0], [0.0, 0.0]]
        for i in range(0, len(expected)):
            self.assertTrue(all(observed[i]["features"].toArray() == expected[i]))

    def test_string_indexer_handle_invalid(self):
        df = self.spark.createDataFrame([(0, "a"), (1, "d"), (2, None)], ["id", "label"])

        si1 = StringIndexer(
            inputCol="label",
            outputCol="indexed",
            handleInvalid="keep",
            stringOrderType="alphabetAsc",
        )
        model1 = si1.fit(df)
        td1 = model1.transform(df)
        actual1 = td1.select("id", "indexed").collect()
        expected1 = [Row(id=0, indexed=0.0), Row(id=1, indexed=1.0), Row(id=2, indexed=2.0)]
        self.assertEqual(actual1, expected1)

        si2 = si1.setHandleInvalid("skip")
        model2 = si2.fit(df)
        td2 = model2.transform(df)
        actual2 = td2.select("id", "indexed").collect()
        expected2 = [Row(id=0, indexed=0.0), Row(id=1, indexed=1.0)]
        self.assertEqual(actual2, expected2)

    def test_string_indexer_from_labels(self):
        model = StringIndexerModel.from_labels(
            ["a", "b", "c"], inputCol="label", outputCol="indexed", handleInvalid="keep"
        )
        self.assertEqual(model.labels, ["a", "b", "c"])
        self.assertEqual(model.labelsArray, [("a", "b", "c")])

        df1 = self.spark.createDataFrame(
            [(0, "a"), (1, "c"), (2, None), (3, "b"), (4, "b")], ["id", "label"]
        )

        result1 = model.transform(df1)
        actual1 = result1.select("id", "indexed").collect()
        expected1 = [
            Row(id=0, indexed=0.0),
            Row(id=1, indexed=2.0),
            Row(id=2, indexed=3.0),
            Row(id=3, indexed=1.0),
            Row(id=4, indexed=1.0),
        ]
        self.assertEqual(actual1, expected1)

        model_empty_labels = StringIndexerModel.from_labels(
            [], inputCol="label", outputCol="indexed", handleInvalid="keep"
        )
        actual2 = model_empty_labels.transform(df1).select("id", "indexed").collect()
        expected2 = [
            Row(id=0, indexed=0.0),
            Row(id=1, indexed=0.0),
            Row(id=2, indexed=0.0),
            Row(id=3, indexed=0.0),
            Row(id=4, indexed=0.0),
        ]
        self.assertEqual(actual2, expected2)

        # Test model with default settings can transform
        model_default = StringIndexerModel.from_labels(["a", "b", "c"], inputCol="label")
        df2 = self.spark.createDataFrame(
            [(0, "a"), (1, "c"), (2, "b"), (3, "b"), (4, "b")], ["id", "label"]
        )
        transformed_list = (
            model_default.transform(df2)
            .select(model_default.getOrDefault(model_default.outputCol))
            .collect()
        )
        self.assertEqual(len(transformed_list), 5)

    def test_vector_size_hint(self):
        df = self.spark.createDataFrame(
            [
                (0, Vectors.dense([0.0, 10.0, 0.5])),
                (1, Vectors.dense([1.0, 11.0, 0.5, 0.6])),
                (2, Vectors.dense([2.0, 12.0])),
            ],
            ["id", "vector"],
        )

        sizeHint = VectorSizeHint(inputCol="vector", handleInvalid="skip")
        sizeHint.setSize(3)
        self.assertEqual(sizeHint.getSize(), 3)

        output = sizeHint.transform(df).head().vector
        expected = DenseVector([0.0, 10.0, 0.5])
        self.assertEqual(output, expected)


class HashingTFTest(SparkSessionTestCase):
    def test_apply_binary_term_freqs(self):

        df = self.spark.createDataFrame([(0, ["a", "a", "b", "c", "c", "c"])], ["id", "words"])
        n = 10
        hashingTF = HashingTF()
        hashingTF.setInputCol("words").setOutputCol("features").setNumFeatures(n).setBinary(True)
        output = hashingTF.transform(df)
        features = output.select("features").first().features.toArray()
        expected = Vectors.dense([0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0]).toArray()
        for i in range(0, n):
            self.assertAlmostEqual(
                features[i],
                expected[i],
                14,
                "Error at "
                + str(i)
                + ": expected "
                + str(expected[i])
                + ", got "
                + str(features[i]),
            )


if __name__ == "__main__":
    from pyspark.ml.tests.test_feature import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
