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

import tempfile
import unittest
from typing import List, Tuple, Any

import numpy as np

from pyspark.ml.feature import (
    Binarizer,
    CountVectorizer,
    CountVectorizerModel,
    HashingTF,
    IDF,
    NGram,
    RFormula,
    StandardScaler,
    StandardScalerModel,
    MaxAbsScaler,
    MaxAbsScalerModel,
    MinMaxScaler,
    MinMaxScalerModel,
    RobustScaler,
    RobustScalerModel,
    StopWordsRemover,
    StringIndexer,
    StringIndexerModel,
    TargetEncoder,
    VectorSizeHint,
    VectorAssembler,
    PCA,
    PCAModel,
)
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors
from pyspark.sql import Row
from pyspark.testing.utils import QuietTest
from pyspark.testing.mlutils import check_params, SparkSessionTestCase


class FeatureTestsMixin:
    def test_string_indexer(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, "a", "e"),
                    (2, "b", "f"),
                    (3, "c", "e"),
                    (4, "a", "f"),
                    (5, "a", "f"),
                    (6, "c", "f"),
                ],
                ["id", "label1", "label2"],
            )
            .coalesce(1)
            .sortWithinPartitions("id")
        )
        # single input
        si = StringIndexer(inputCol="label1", outputCol="index1")
        model = si.fit(df.select("label1"))

        # read/write
        with tempfile.TemporaryDirectory(prefix="read_write") as tmp_dir:
            si.write().overwrite().save(tmp_dir)
            si2 = StringIndexer.load(tmp_dir)
            self.assertEqual(str(si), str(si2))
            self.assertEqual(si.getInputCol(), "label1")
            self.assertEqual(si2.getInputCol(), "label1")

            model.write().overwrite().save(tmp_dir)
            model2 = StringIndexerModel.load(tmp_dir)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.getInputCol(), "label1")
            self.assertEqual(model.getOutputCol(), "index1")
            self.assertEqual(model2.getInputCol(), "label1")

        indexed_df = model.transform(df.select("label1"))
        self.assertEqual(sorted(indexed_df.schema.names), sorted(["label1", "index1"]))

        def check_a_b(result: List[Tuple[Any, Any]]) -> None:
            self.assertTrue(result[0][0] == "a" and result[1][0] == "b" and result[2][0] == "c")
            sorted_value = sorted([v for _, v in result])
            self.assertEqual(sorted_value, [0.0, 1.0, 2.0])

        check_a_b(sorted(set([(i[0], i[1]) for i in indexed_df.collect()]), key=lambda x: x[0]))

        # multiple inputs
        input_cols = ["label1", "label2"]
        output_cols = ["index1", "index2"]
        si = StringIndexer(inputCols=input_cols, outputCols=output_cols)
        model = si.fit(df.select(*input_cols))
        self.assertEqual(model.getInputCols(), input_cols)
        self.assertEqual(model.getOutputCols(), output_cols)

        indexed_df = model.transform(df.select(*input_cols))
        self.assertEqual(
            sorted(indexed_df.schema.names), sorted(["label1", "index1", "label2", "index2"])
        )

        rows = indexed_df.collect()
        check_a_b(sorted(set([(i[0], i[2]) for i in rows]), key=lambda x: x[0]))

        # check e f
        result = sorted(set([(i[1], i[3]) for i in rows]), key=lambda x: x[0])
        self.assertTrue(result[0][0] == "e" and result[1][0] == "f")
        sorted_value = sorted([v for _, v in result])
        self.assertEqual(sorted_value, [0.0, 1.0])

    def test_pca(self):
        df = self.spark.createDataFrame(
            [
                (Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
                (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
                (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),),
            ],
            ["features"],
        )
        pca = PCA(k=2, inputCol="features", outputCol="pca_features")

        model = pca.fit(df)
        self.assertEqual(model.getK(), 2)
        self.assertTrue(
            np.allclose(model.explainedVariance.toArray(), [0.79439, 0.20560], atol=1e-4)
        )
        model.setOutputCol("output")
        # Transform the data using the PCA model
        transformed_df = model.transform(df)
        self.assertTrue(
            np.allclose(
                transformed_df.collect()[0].output.toArray(), [1.64857, -4.013282], atol=1e-4
            )
        )

        # read/write
        with tempfile.TemporaryDirectory(prefix="read_write") as tmp_dir:
            pca.write().overwrite().save(tmp_dir)
            pca2 = PCA.load(tmp_dir)
            self.assertEqual(str(pca), str(pca2))
            self.assertEqual(pca.getInputCol(), "features")
            self.assertEqual(pca2.getInputCol(), "features")

            model.write().overwrite().save(tmp_dir)
            model2 = PCAModel.load(tmp_dir)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.getInputCol(), "features")
            self.assertEqual(model2.getInputCol(), "features")

    def test_vector_assembler(self):
        # Create a DataFrame
        df = (
            self.spark.createDataFrame(
                [
                    (1, 5.0, 6.0, 7.0),
                    (2, 1.0, 2.0, None),
                    (3, 3.0, float("nan"), 4.0),
                ],
                ["index", "a", "b", "c"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
        )

        # Initialize VectorAssembler
        vec_assembler = VectorAssembler(outputCol="features").setInputCols(["a", "b", "c"])
        output = vec_assembler.transform(df)
        self.assertEqual(output.columns, ["index", "a", "b", "c", "features"])
        self.assertEqual(output.head().features, Vectors.dense([5.0, 6.0, 7.0]))

        # Set custom parameters and transform the DataFrame
        params = {vec_assembler.inputCols: ["b", "a"], vec_assembler.outputCol: "vector"}
        self.assertEqual(
            vec_assembler.transform(df, params).head().vector, Vectors.dense([6.0, 5.0])
        )

        # read/write
        with tempfile.TemporaryDirectory(prefix="read_write") as tmp_dir:
            vec_assembler.write().overwrite().save(tmp_dir)
            vec_assembler2 = VectorAssembler.load(tmp_dir)
            self.assertEqual(str(vec_assembler), str(vec_assembler2))

        # Initialize a new VectorAssembler with handleInvalid="keep"
        vec_assembler3 = VectorAssembler(
            inputCols=["a", "b", "c"], outputCol="features", handleInvalid="keep"
        )
        self.assertEqual(vec_assembler3.transform(df).count(), 3)

        # Update handleInvalid to "skip" and transform the DataFrame
        vec_assembler3.setParams(handleInvalid="skip")
        self.assertEqual(vec_assembler3.transform(df).count(), 1)

    def test_standard_scaler(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([0.0])),
                    (2, 2.0, Vectors.dense([2.0])),
                    (3, 3.0, Vectors.sparse(1, [(0, 3.0)])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("features")
        )
        scaler = StandardScaler(inputCol="features", outputCol="scaled")
        self.assertEqual(scaler.getInputCol(), "features")
        self.assertEqual(scaler.getOutputCol(), "scaled")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="standard_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = StandardScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))

        model = scaler.fit(df)
        self.assertTrue(np.allclose(model.mean.toArray(), [1.66666667], atol=1e-4))
        self.assertTrue(np.allclose(model.std.toArray(), [1.52752523], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="standard_scaler_model") as d:
            model.write().overwrite().save(d)
            model2 = StandardScalerModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_maxabs_scaler(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([0.0])),
                    (2, 2.0, Vectors.dense([2.0])),
                    (3, 3.0, Vectors.sparse(1, [(0, 3.0)])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("features")
        )

        scaler = MaxAbsScaler(inputCol="features", outputCol="scaled")
        self.assertEqual(scaler.getInputCol(), "features")
        self.assertEqual(scaler.getOutputCol(), "scaled")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="maxabs_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = MaxAbsScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))

        model = scaler.fit(df)
        self.assertTrue(np.allclose(model.maxAbs.toArray(), [3.0], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="standard_scaler_model") as d:
            model.write().overwrite().save(d)
            model2 = MaxAbsScalerModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_minmax_scaler(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([0.0])),
                    (2, 2.0, Vectors.dense([2.0])),
                    (3, 3.0, Vectors.sparse(1, [(0, 3.0)])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("features")
        )

        scaler = MinMaxScaler(inputCol="features", outputCol="scaled")
        self.assertEqual(scaler.getInputCol(), "features")
        self.assertEqual(scaler.getOutputCol(), "scaled")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="maxabs_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = MinMaxScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))

        model = scaler.fit(df)
        self.assertTrue(np.allclose(model.originalMax.toArray(), [3.0], atol=1e-4))
        self.assertTrue(np.allclose(model.originalMin.toArray(), [0.0], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="standard_scaler_model") as d:
            model.write().overwrite().save(d)
            model2 = MinMaxScalerModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_robust_scaler(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([0.0])),
                    (2, 2.0, Vectors.dense([2.0])),
                    (3, 3.0, Vectors.sparse(1, [(0, 3.0)])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("features")
        )

        scaler = RobustScaler(inputCol="features", outputCol="scaled")
        self.assertEqual(scaler.getInputCol(), "features")
        self.assertEqual(scaler.getOutputCol(), "scaled")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="robust_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = RobustScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))

        model = scaler.fit(df)
        self.assertTrue(np.allclose(model.range.toArray(), [3.0], atol=1e-4))
        self.assertTrue(np.allclose(model.median.toArray(), [2.0], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="robust_scaler_model") as d:
            model.write().overwrite().save(d)
            model2 = RobustScalerModel.load(d)
            self.assertEqual(str(model), str(model2))

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

    def test_target_encoder_binary(self):
        df = self.spark.createDataFrame(
            [
                (0, 3, 5.0, 0.0),
                (1, 4, 5.0, 1.0),
                (2, 3, 5.0, 0.0),
                (0, 4, 6.0, 1.0),
                (1, 3, 6.0, 0.0),
                (2, 4, 6.0, 1.0),
                (0, 3, 7.0, 0.0),
                (1, 4, 8.0, 1.0),
                (2, 3, 9.0, 0.0),
            ],
            schema="input1 short, input2 int, input3 double, label double",
        )
        encoder = TargetEncoder(
            inputCols=["input1", "input2", "input3"],
            outputCols=["output", "output2", "output3"],
            labelCol="label",
            targetType="binary",
        )
        model = encoder.fit(df)
        te = model.transform(df)
        actual = te.drop("label").collect()
        expected = [
            Row(input1=0, input2=3, input3=5.0, output1=1.0 / 3, output2=0.0, output3=1.0 / 3),
            Row(input1=1, input2=4, input3=5.0, output1=2.0 / 3, output2=1.0, output3=1.0 / 3),
            Row(input1=2, input2=3, input3=5.0, output1=1.0 / 3, output2=0.0, output3=1.0 / 3),
            Row(input1=0, input2=4, input3=6.0, output1=1.0 / 3, output2=1.0, output3=2.0 / 3),
            Row(input1=1, input2=3, input3=6.0, output1=2.0 / 3, output2=0.0, output3=2.0 / 3),
            Row(input1=2, input2=4, input3=6.0, output1=1.0 / 3, output2=1.0, output3=2.0 / 3),
            Row(input1=0, input2=3, input3=7.0, output1=1.0 / 3, output2=0.0, output3=0.0),
            Row(input1=1, input2=4, input3=8.0, output1=2.0 / 3, output2=1.0, output3=1.0),
            Row(input1=2, input2=3, input3=9.0, output1=1.0 / 3, output2=0.0, output3=0.0),
        ]
        self.assertEqual(actual, expected)
        te = model.setSmoothing(1.0).transform(df)
        actual = te.drop("label").collect()
        expected = [
            Row(
                input1=0,
                input2=3,
                input3=5.0,
                output1=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(1 - 5 / 6) * (4 / 9),
                output3=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
            ),
            Row(
                input1=1,
                input2=4,
                input3=5.0,
                output1=(3 / 4) * (2 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(4 / 5) * 1 + (1 - 4 / 5) * (4 / 9),
                output3=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
            ),
            Row(
                input1=2,
                input2=3,
                input3=5.0,
                output1=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(1 - 5 / 6) * (4 / 9),
                output3=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
            ),
            Row(
                input1=0,
                input2=4,
                input3=6.0,
                output1=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(4 / 5) * 1 + (1 - 4 / 5) * (4 / 9),
                output3=(3 / 4) * (2 / 3) + (1 - 3 / 4) * (4 / 9),
            ),
            Row(
                input1=1,
                input2=3,
                input3=6.0,
                output1=(3 / 4) * (2 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(1 - 5 / 6) * (4 / 9),
                output3=(3 / 4) * (2 / 3) + (1 - 3 / 4) * (4 / 9),
            ),
            Row(
                input1=2,
                input2=4,
                input3=6.0,
                output1=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(4 / 5) * 1 + (1 - 4 / 5) * (4 / 9),
                output3=(3 / 4) * (2 / 3) + (1 - 3 / 4) * (4 / 9),
            ),
            Row(
                input1=0,
                input2=3,
                input3=7.0,
                output1=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(1 - 5 / 6) * (4 / 9),
                output3=(1 - 1 / 2) * (4 / 9),
            ),
            Row(
                input1=1,
                input2=4,
                input3=8.0,
                output1=(3 / 4) * (2 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(4 / 5) * 1 + (1 - 4 / 5) * (4 / 9),
                output3=(1 / 2) + (1 - 1 / 2) * (4 / 9),
            ),
            Row(
                input1=2,
                input2=3,
                input3=9.0,
                output1=(3 / 4) * (1 / 3) + (1 - 3 / 4) * (4 / 9),
                output2=(1 - 5 / 6) * (4 / 9),
                output3=(1 - 1 / 2) * (4 / 9),
            ),
        ]
        self.assertEqual(actual, expected)

    def test_target_encoder_continuous(self):
        df = self.spark.createDataFrame(
            [
                (0, 3, 5.0, 10.0),
                (1, 4, 5.0, 20.0),
                (2, 3, 5.0, 30.0),
                (0, 4, 6.0, 40.0),
                (1, 3, 6.0, 50.0),
                (2, 4, 6.0, 60.0),
                (0, 3, 7.0, 70.0),
                (1, 4, 8.0, 80.0),
                (2, 3, 9.0, 90.0),
            ],
            schema="input1 short, input2 int, input3 double, label double",
        )
        encoder = TargetEncoder(
            inputCols=["input1", "input2", "input3"],
            outputCols=["output", "output2", "output3"],
            labelCol="label",
            targetType="continuous",
        )
        model = encoder.fit(df)
        te = model.transform(df)
        actual = te.drop("label").collect()
        expected = [
            Row(input1=0, input2=3, input3=5.0, output1=40.0, output2=50.0, output3=20.0),
            Row(input1=1, input2=4, input3=5.0, output1=50.0, output2=50.0, output3=20.0),
            Row(input1=2, input2=3, input3=5.0, output1=60.0, output2=50.0, output3=20.0),
            Row(input1=0, input2=4, input3=6.0, output1=40.0, output2=50.0, output3=50.0),
            Row(input1=1, input2=3, input3=6.0, output1=50.0, output2=50.0, output3=50.0),
            Row(input1=2, input2=4, input3=6.0, output1=60.0, output2=50.0, output3=50.0),
            Row(input1=0, input2=3, input3=7.0, output1=40.0, output2=50.0, output3=70.0),
            Row(input1=1, input2=4, input3=8.0, output1=50.0, output2=50.0, output3=80.0),
            Row(input1=2, input2=3, input3=9.0, output1=60.0, output2=50.0, output3=90.0),
        ]
        self.assertEqual(actual, expected)
        te = model.setSmoothing(1.0).transform(df)
        actual = te.drop("label").collect()
        expected = [
            Row(input1=0, input2=3, input3=5.0, output1=42.5, output2=50.0, output3=27.5),
            Row(input1=1, input2=4, input3=5.0, output1=50.0, output2=50.0, output3=27.5),
            Row(input1=2, input2=3, input3=5.0, output1=57.5, output2=50.0, output3=27.5),
            Row(input1=0, input2=4, input3=6.0, output1=42.5, output2=50.0, output3=50.0),
            Row(input1=1, input2=3, input3=6.0, output1=50.0, output2=50.0, output3=50.0),
            Row(input1=2, input2=4, input3=6.0, output1=57.5, output2=50.0, output3=50.0),
            Row(input1=0, input2=3, input3=7.0, output1=42.5, output2=50.0, output3=60.0),
            Row(input1=1, input2=4, input3=8.0, output1=50.0, output2=50.0, output3=65.0),
            Row(input1=2, input2=3, input3=9.0, output1=57.5, output2=50.0, output3=70.0),
        ]
        self.assertEqual(actual, expected)

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


class FeatureTests(FeatureTestsMixin, SparkSessionTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_feature import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
