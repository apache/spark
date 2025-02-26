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
    DCT,
    Binarizer,
    Bucketizer,
    QuantileDiscretizer,
    CountVectorizer,
    CountVectorizerModel,
    OneHotEncoder,
    OneHotEncoderModel,
    FeatureHasher,
    ElementwiseProduct,
    HashingTF,
    IDF,
    IDFModel,
    Imputer,
    ImputerModel,
    NGram,
    Normalizer,
    Interaction,
    RFormula,
    RFormulaModel,
    Tokenizer,
    SQLTransformer,
    RegexTokenizer,
    StandardScaler,
    StandardScalerModel,
    MaxAbsScaler,
    MaxAbsScalerModel,
    MinMaxScaler,
    MinMaxScalerModel,
    RobustScaler,
    RobustScalerModel,
    ChiSqSelector,
    ChiSqSelectorModel,
    UnivariateFeatureSelector,
    UnivariateFeatureSelectorModel,
    VarianceThresholdSelector,
    VarianceThresholdSelectorModel,
    StopWordsRemover,
    StringIndexer,
    StringIndexerModel,
    VectorIndexer,
    VectorIndexerModel,
    TargetEncoder,
    TargetEncoderModel,
    VectorSizeHint,
    VectorSlicer,
    VectorAssembler,
    PCA,
    PCAModel,
    Word2Vec,
    Word2VecModel,
    BucketedRandomProjectionLSH,
    BucketedRandomProjectionLSHModel,
    MinHashLSH,
    MinHashLSHModel,
    IndexToString,
    PolynomialExpansion,
)
from pyspark.ml.linalg import DenseVector, SparseVector, Vectors
from pyspark.sql import Row
from pyspark.testing.sqlutils import ReusedSQLTestCase


class FeatureTestsMixin:
    def test_polynomial_expansion(self):
        df = self.spark.createDataFrame([(Vectors.dense([0.5, 2.0]),)], ["dense"])
        px = PolynomialExpansion(degree=2)
        px.setInputCol("dense")
        px.setOutputCol("expanded")
        self.assertTrue(
            np.allclose(
                px.transform(df).head().expanded.toArray(), [0.5, 0.25, 2.0, 1.0, 4.0], atol=1e-4
            )
        )

        def check(p: PolynomialExpansion) -> None:
            self.assertEqual(p.getInputCol(), "dense")
            self.assertEqual(p.getOutputCol(), "expanded")
            self.assertEqual(p.getDegree(), 2)

        check(px)

        # save & load
        with tempfile.TemporaryDirectory(prefix="px") as d:
            px.write().overwrite().save(d)
            px2 = PolynomialExpansion.load(d)
            self.assertEqual(str(px), str(px2))
            check(px2)

    def test_index_string(self):
        dataset = self.spark.createDataFrame(
            [
                (0, "a"),
                (1, "b"),
                (2, "c"),
                (3, "a"),
                (4, "a"),
                (5, "c"),
            ],
            ["id", "label"],
        )

        indexer = StringIndexer(inputCol="label", outputCol="labelIndex").fit(dataset)
        transformed = indexer.transform(dataset)
        idx2str = (
            IndexToString()
            .setInputCol("labelIndex")
            .setOutputCol("sameLabel")
            .setLabels(indexer.labels)
        )

        def check(t: IndexToString) -> None:
            self.assertEqual(t.getInputCol(), "labelIndex")
            self.assertEqual(t.getOutputCol(), "sameLabel")
            self.assertEqual(t.getLabels(), indexer.labels)

        check(idx2str)

        ret = idx2str.transform(transformed)
        self.assertEqual(
            sorted(ret.schema.names), sorted(["id", "label", "labelIndex", "sameLabel"])
        )

        rows = ret.select("label", "sameLabel").collect()
        for r in rows:
            self.assertEqual(r.label, r.sameLabel)

        # save & load
        with tempfile.TemporaryDirectory(prefix="index_string") as d:
            idx2str.write().overwrite().save(d)
            idx2str2 = IndexToString.load(d)
            self.assertEqual(str(idx2str), str(idx2str2))
            check(idx2str2)

    def test_dct(self):
        df = self.spark.createDataFrame([(Vectors.dense([5.0, 8.0, 6.0]),)], ["vec"])
        dct = DCT()
        dct.setInverse(False)
        dct.setInputCol("vec")
        dct.setOutputCol("resultVec")

        self.assertFalse(dct.getInverse())
        self.assertEqual(dct.getInputCol(), "vec")
        self.assertEqual(dct.getOutputCol(), "resultVec")

        output = dct.transform(df)
        self.assertEqual(output.columns, ["vec", "resultVec"])
        self.assertEqual(output.count(), 1)
        self.assertTrue(
            np.allclose(
                output.head().resultVec.toArray(),
                [10.96965511, -0.70710678, -2.04124145],
                atol=1e-4,
            )
        )

        # save & load
        with tempfile.TemporaryDirectory(prefix="dct") as d:
            dct.write().overwrite().save(d)
            dct2 = DCT.load(d)
            self.assertEqual(str(dct), str(dct2))

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
        self.assertEqual(si.uid, model.uid)
        self.assertEqual(model.labels, list(model.labelsArray[0]))

        # read/write
        with tempfile.TemporaryDirectory(prefix="string_indexer") as tmp_dir:
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

    def test_vector_indexer(self):
        spark = self.spark
        df = spark.createDataFrame(
            [
                (Vectors.dense([-1.0, 0.0]),),
                (Vectors.dense([0.0, 1.0]),),
                (Vectors.dense([0.0, 2.0]),),
            ],
            ["a"],
        )

        indexer = VectorIndexer(maxCategories=2, inputCol="a")
        indexer.setOutputCol("indexed")
        self.assertEqual(indexer.getMaxCategories(), 2)
        self.assertEqual(indexer.getInputCol(), "a")
        self.assertEqual(indexer.getOutputCol(), "indexed")

        model = indexer.fit(df)
        self.assertEqual(indexer.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)

        categoryMaps = model.categoryMaps
        self.assertEqual(categoryMaps, {0: {0.0: 0, -1.0: 1}}, categoryMaps)

        output = model.transform(df)
        self.assertEqual(output.columns, ["a", "indexed"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="vector_indexer") as d:
            indexer.write().overwrite().save(d)
            indexer2 = VectorIndexer.load(d)
            self.assertEqual(str(indexer), str(indexer2))
            self.assertEqual(indexer2.getOutputCol(), "indexed")

            model.write().overwrite().save(d)
            model2 = VectorIndexerModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model2.getOutputCol(), "indexed")

    def test_elementwise_product(self):
        spark = self.spark
        df = spark.createDataFrame([(Vectors.dense([2.0, 1.0, 3.0]),)], ["values"])

        ep = ElementwiseProduct()
        ep.setScalingVec(Vectors.dense([1.0, 2.0, 3.0]))
        ep.setInputCol("values")
        ep.setOutputCol("eprod")

        self.assertEqual(ep.getScalingVec(), Vectors.dense([1.0, 2.0, 3.0]))
        self.assertEqual(ep.getInputCol(), "values")
        self.assertEqual(ep.getOutputCol(), "eprod")

        output = ep.transform(df)
        self.assertEqual(output.columns, ["values", "eprod"])
        self.assertEqual(output.count(), 1)

        # save & load
        with tempfile.TemporaryDirectory(prefix="elementwise_product") as d:
            ep.write().overwrite().save(d)
            ep2 = ElementwiseProduct.load(d)
            self.assertEqual(str(ep), str(ep2))
            self.assertEqual(ep2.getOutputCol(), "eprod")

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
        self.assertTrue(np.allclose(model.pc.toArray()[0], [-0.44859172, -0.28423808], atol=1e-4))
        self.assertEqual(pca.uid, model.uid)
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
        with tempfile.TemporaryDirectory(prefix="pca") as tmp_dir:
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
        with tempfile.TemporaryDirectory(prefix="vector_assembler") as tmp_dir:
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

        model = scaler.fit(df)
        self.assertEqual(scaler.uid, model.uid)
        self.assertTrue(np.allclose(model.mean.toArray(), [1.66666667], atol=1e-4))
        self.assertTrue(np.allclose(model.std.toArray(), [1.52752523], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="standard_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = StandardScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))
            self.assertEqual(scaler2.getOutputCol(), "scaled")

            model.write().overwrite().save(d)
            model2 = StandardScalerModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model2.getOutputCol(), "scaled")

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

        model = scaler.fit(df)
        self.assertEqual(scaler.uid, model.uid)
        self.assertTrue(np.allclose(model.maxAbs.toArray(), [3.0], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="standard_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = MaxAbsScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))
            self.assertEqual(scaler2.getOutputCol(), "scaled")

            model.write().overwrite().save(d)
            model2 = MaxAbsScalerModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model2.getOutputCol(), "scaled")

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

        model = scaler.fit(df)
        self.assertEqual(scaler.uid, model.uid)
        self.assertTrue(np.allclose(model.originalMax.toArray(), [3.0], atol=1e-4))
        self.assertTrue(np.allclose(model.originalMin.toArray(), [0.0], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="min_max_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = MinMaxScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))
            self.assertEqual(scaler2.getOutputCol(), "scaled")

            model.write().overwrite().save(d)
            model2 = MinMaxScalerModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model2.getOutputCol(), "scaled")

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

        model = scaler.fit(df)
        self.assertEqual(scaler.uid, model.uid)
        self.assertTrue(np.allclose(model.range.toArray(), [3.0], atol=1e-4))
        self.assertTrue(np.allclose(model.median.toArray(), [2.0], atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "scaled"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="robust_scaler") as d:
            scaler.write().overwrite().save(d)
            scaler2 = RobustScaler.load(d)
            self.assertEqual(str(scaler), str(scaler2))
            self.assertEqual(scaler2.getOutputCol(), "scaled")

            model.write().overwrite().save(d)
            model2 = RobustScalerModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model2.getOutputCol(), "scaled")

    def test_chi_sq_selector(self):
        df = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0),
                (Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0),
                (Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0),
            ],
            ["features", "label"],
        )

        selector = ChiSqSelector(numTopFeatures=1, outputCol="selectedFeatures")
        self.assertEqual(selector.getNumTopFeatures(), 1)
        self.assertEqual(selector.getOutputCol(), "selectedFeatures")

        model = selector.fit(df)
        self.assertEqual(selector.uid, model.uid)
        self.assertEqual(model.selectedFeatures, [2])

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "label", "selectedFeatures"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="chi_sq_selector") as d:
            selector.write().overwrite().save(d)
            selector2 = ChiSqSelector.load(d)
            self.assertEqual(str(selector), str(selector2))

            model.write().overwrite().save(d)
            model2 = ChiSqSelectorModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_univariate_selector(self):
        df = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0),
                (Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0),
                (Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0),
            ],
            ["features", "label"],
        )

        selector = UnivariateFeatureSelector(outputCol="selectedFeatures")
        selector.setFeatureType("continuous").setLabelType("categorical").setSelectionThreshold(1)
        self.assertEqual(selector.getFeatureType(), "continuous")
        self.assertEqual(selector.getLabelType(), "categorical")
        self.assertEqual(selector.getOutputCol(), "selectedFeatures")
        self.assertEqual(selector.getSelectionThreshold(), 1)

        model = selector.fit(df)
        self.assertEqual(selector.uid, model.uid)
        self.assertEqual(model.selectedFeatures, [3])

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "label", "selectedFeatures"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="univariate_selector") as d:
            selector.write().overwrite().save(d)
            selector2 = UnivariateFeatureSelector.load(d)
            self.assertEqual(str(selector), str(selector2))

            model.write().overwrite().save(d)
            model2 = UnivariateFeatureSelectorModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_variance_threshold_selector(self):
        df = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0, 0.0, 18.0, 1.0]), 1.0),
                (Vectors.dense([0.0, 1.0, 12.0, 0.0]), 0.0),
                (Vectors.dense([1.0, 0.0, 15.0, 0.1]), 0.0),
            ],
            ["features", "label"],
        )

        selector = VarianceThresholdSelector(varianceThreshold=2, outputCol="selectedFeatures")
        self.assertEqual(selector.getVarianceThreshold(), 2)
        self.assertEqual(selector.getOutputCol(), "selectedFeatures")

        model = selector.fit(df)
        self.assertEqual(selector.uid, model.uid)
        self.assertEqual(model.selectedFeatures, [2])

        output = model.transform(df)
        self.assertEqual(output.columns, ["features", "label", "selectedFeatures"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="variance_threshold_selector") as d:
            selector.write().overwrite().save(d)
            selector2 = VarianceThresholdSelector.load(d)
            self.assertEqual(str(selector), str(selector2))

            model.write().overwrite().save(d)
            model2 = VarianceThresholdSelectorModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_word2vec(self):
        sent = ("a b " * 100 + "a c " * 10).split(" ")
        df = self.spark.createDataFrame([(sent,), (sent,)], ["sentence"]).coalesce(1)

        w2v = Word2Vec(vectorSize=3, seed=42, inputCol="sentence", outputCol="model")
        w2v.setMaxIter(1)
        self.assertEqual(w2v.getInputCol(), "sentence")
        self.assertEqual(w2v.getOutputCol(), "model")
        self.assertEqual(w2v.getVectorSize(), 3)
        self.assertEqual(w2v.getSeed(), 42)
        self.assertEqual(w2v.getMaxIter(), 1)

        model = w2v.fit(df)
        self.assertEqual(w2v.uid, model.uid)
        self.assertEqual(model.getVectors().columns, ["word", "vector"])
        self.assertEqual(model.getVectors().count(), 3)

        synonyms = model.findSynonyms("a", 2)
        self.assertEqual(synonyms.columns, ["word", "similarity"])
        self.assertEqual(synonyms.count(), 2)

        synonyms = model.findSynonymsArray("a", 2)
        self.assertEqual(len(synonyms), 2)
        self.assertEqual(synonyms[0][0], "b")
        self.assertTrue(np.allclose(synonyms[0][1], -0.024012837558984756, atol=1e-4))
        self.assertEqual(synonyms[1][0], "c")
        self.assertTrue(np.allclose(synonyms[1][1], -0.19355154037475586, atol=1e-4))

        output = model.transform(df)
        self.assertEqual(output.columns, ["sentence", "model"])
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="word2vec") as d:
            w2v.write().overwrite().save(d)
            w2v2 = Word2Vec.load(d)
            self.assertEqual(str(w2v), str(w2v2))

            model.write().overwrite().save(d)
            model2 = Word2VecModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_imputer(self):
        spark = self.spark
        df = spark.createDataFrame(
            [
                (1.0, float("nan")),
                (2.0, float("nan")),
                (float("nan"), 3.0),
                (4.0, 4.0),
                (5.0, 5.0),
            ],
            ["a", "b"],
        )

        imputer = Imputer(strategy="mean")
        imputer.setInputCols(["a", "b"])
        imputer.setOutputCols(["out_a", "out_b"])

        self.assertEqual(imputer.getStrategy(), "mean")
        self.assertEqual(imputer.getInputCols(), ["a", "b"])
        self.assertEqual(imputer.getOutputCols(), ["out_a", "out_b"])

        model = imputer.fit(df)
        self.assertEqual(imputer.uid, model.uid)
        self.assertEqual(model.surrogateDF.columns, ["a", "b"])
        self.assertEqual(model.surrogateDF.count(), 1)
        self.assertEqual(list(model.surrogateDF.head()), [3.0, 4.0])

        output = model.transform(df)
        self.assertEqual(output.columns, ["a", "b", "out_a", "out_b"])
        self.assertEqual(output.count(), 5)

        # save & load
        with tempfile.TemporaryDirectory(prefix="imputer") as d:
            imputer.write().overwrite().save(d)
            imputer2 = Imputer.load(d)
            self.assertEqual(str(imputer), str(imputer2))

            model.write().overwrite().save(d)
            model2 = ImputerModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_count_vectorizer(self):
        df = self.spark.createDataFrame(
            [(0, ["a", "b", "c"]), (1, ["a", "b", "b", "c", "a"])],
            ["label", "raw"],
        )

        cv = CountVectorizer()
        cv.setInputCol("raw")
        cv.setOutputCol("vectors")
        self.assertEqual(cv.getInputCol(), "raw")
        self.assertEqual(cv.getOutputCol(), "vectors")

        model = cv.fit(df)
        self.assertEqual(cv.uid, model.uid)
        self.assertEqual(sorted(model.vocabulary), ["a", "b", "c"])

        output = model.transform(df)
        self.assertEqual(output.columns, ["label", "raw", "vectors"])
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="count_vectorizer") as d:
            cv.write().overwrite().save(d)
            cv2 = CountVectorizer.load(d)
            self.assertEqual(str(cv), str(cv2))

            model.write().overwrite().save(d)
            model2 = CountVectorizerModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_one_hot_encoder(self):
        df = self.spark.createDataFrame([(0.0,), (1.0,), (2.0,)], ["input"])

        encoder = OneHotEncoder()
        encoder.setInputCols(["input"])
        encoder.setOutputCols(["output"])
        self.assertEqual(encoder.getInputCols(), ["input"])
        self.assertEqual(encoder.getOutputCols(), ["output"])

        model = encoder.fit(df)
        self.assertEqual(encoder.uid, model.uid)
        self.assertEqual(model.categorySizes, [3])

        output = model.transform(df)
        self.assertEqual(output.columns, ["input", "output"])
        self.assertEqual(output.count(), 3)

        # save & load
        with tempfile.TemporaryDirectory(prefix="count_vectorizer") as d:
            encoder.write().overwrite().save(d)
            encoder2 = OneHotEncoder.load(d)
            self.assertEqual(str(encoder), str(encoder2))

            model.write().overwrite().save(d)
            model2 = OneHotEncoderModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_tokenizer(self):
        df = self.spark.createDataFrame([("a b c",)], ["text"])

        tokenizer = Tokenizer(outputCol="words")
        tokenizer.setInputCol("text")
        self.assertEqual(tokenizer.getInputCol(), "text")
        self.assertEqual(tokenizer.getOutputCol(), "words")

        output = tokenizer.transform(df)
        self.assertEqual(output.columns, ["text", "words"])
        self.assertEqual(output.count(), 1)
        self.assertEqual(output.head().words, ["a", "b", "c"])

        # save & load
        with tempfile.TemporaryDirectory(prefix="tokenizer") as d:
            tokenizer.write().overwrite().save(d)
            tokenizer2 = Tokenizer.load(d)
            self.assertEqual(str(tokenizer), str(tokenizer2))

    def test_regex_tokenizer(self):
        df = self.spark.createDataFrame([("A B  c",)], ["text"])

        tokenizer = RegexTokenizer(outputCol="words")
        tokenizer.setInputCol("text")
        self.assertEqual(tokenizer.getInputCol(), "text")
        self.assertEqual(tokenizer.getOutputCol(), "words")

        output = tokenizer.transform(df)
        self.assertEqual(output.columns, ["text", "words"])
        self.assertEqual(output.count(), 1)
        self.assertEqual(output.head().words, ["a", "b", "c"])

        # save & load
        with tempfile.TemporaryDirectory(prefix="regex_tokenizer") as d:
            tokenizer.write().overwrite().save(d)
            tokenizer2 = RegexTokenizer.load(d)
            self.assertEqual(str(tokenizer), str(tokenizer2))

    def test_sql_transformer(self):
        df = self.spark.createDataFrame([(0, 1.0, 3.0), (2, 2.0, 5.0)], ["id", "v1", "v2"])

        statement = "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__"
        sql = SQLTransformer(statement=statement)
        self.assertEqual(sql.getStatement(), statement)

        output = sql.transform(df)
        self.assertEqual(output.columns, ["id", "v1", "v2", "v3", "v4"])
        self.assertEqual(output.count(), 2)
        self.assertEqual(
            output.collect(),
            [
                Row(id=0, v1=1.0, v2=3.0, v3=4.0, v4=3.0),
                Row(id=2, v1=2.0, v2=5.0, v3=7.0, v4=10.0),
            ],
        )

        # save & load
        with tempfile.TemporaryDirectory(prefix="sql_transformer") as d:
            sql.write().overwrite().save(d)
            sql2 = SQLTransformer.load(d)
            self.assertEqual(str(sql), str(sql2))

    def test_stop_words_remover(self):
        df = self.spark.createDataFrame([(["a", "b", "c"],)], ["text"])

        remover = StopWordsRemover(stopWords=["b"])
        remover.setInputCol("text")
        remover.setOutputCol("words")

        self.assertEqual(remover.getStopWords(), ["b"])
        self.assertEqual(remover.getInputCol(), "text")
        self.assertEqual(remover.getOutputCol(), "words")

        output = remover.transform(df)
        self.assertEqual(output.columns, ["text", "words"])
        self.assertEqual(output.count(), 1)
        self.assertEqual(output.head().words, ["a", "c"])

        # save & load
        with tempfile.TemporaryDirectory(prefix="stop_words_remover") as d:
            remover.write().overwrite().save(d)
            remover2 = StopWordsRemover.load(d)
            self.assertEqual(str(remover), str(remover2))

    def test_stop_words_remover_with_given_words(self):
        spark = self.spark
        dataset = spark.createDataFrame([Row(input=["a", "panda"])])
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
        # with locale
        stopwords = ["BELKİ"]
        dataset = self.spark.createDataFrame([Row(input=["belki"])])
        stopWordRemover.setStopWords(stopwords).setLocale("tr")
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, [])

    def test_stop_words_remover_with_turkish(self):
        spark = self.spark
        dataset = spark.createDataFrame([Row(input=["acaba", "ama", "biri"])])
        stopWordRemover = StopWordsRemover(inputCol="input", outputCol="output")
        stopwords = StopWordsRemover.loadDefaultStopWords("turkish")
        stopWordRemover.setStopWords(stopwords)
        self.assertEqual(stopWordRemover.getStopWords(), stopwords)
        transformedDF = stopWordRemover.transform(dataset)
        self.assertEqual(transformedDF.head().output, [])

    def test_stop_words_remover_default(self):
        stopWordRemover = StopWordsRemover(inputCol="input", outputCol="output")

        # check the default value of local
        locale = stopWordRemover.getLocale()
        self.assertIsInstance(locale, str)
        self.assertTrue(len(locale) > 0)

        # check the default value of stop words
        stopwords = stopWordRemover.getStopWords()
        self.assertIsInstance(stopwords, list)
        self.assertTrue(len(stopwords) > 0)
        self.assertTrue(all(isinstance(word, str) for word in stopwords))

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

        df = self.spark.createDataFrame(
            [
                (0.1, 0.0),
                (0.4, 1.0),
                (1.2, 1.3),
                (1.5, float("nan")),
                (float("nan"), 1.0),
                (float("nan"), 0.0),
            ],
            ["v1", "v2"],
        )

        binarizer = Binarizer(threshold=1.0, inputCol="v1", outputCol="f1")
        output = binarizer.transform(df)
        self.assertEqual(output.columns, ["v1", "v2", "f1"])
        self.assertEqual(output.count(), 6)
        self.assertEqual(
            [r.f1 for r in output.select("f1").collect()],
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
        )

        binarizer = Binarizer(threshold=1.0, inputCols=["v1", "v2"], outputCols=["f1", "f2"])
        output = binarizer.transform(df)
        self.assertEqual(output.columns, ["v1", "v2", "f1", "f2"])
        self.assertEqual(output.count(), 6)
        self.assertEqual(
            [r.f1 for r in output.select("f1").collect()],
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
        )
        self.assertEqual(
            [r.f2 for r in output.select("f2").collect()],
            [0.0, 0.0, 1.0, 0.0, 0.0, 0.0],
        )

        # save & load
        with tempfile.TemporaryDirectory(prefix="binarizer") as d:
            binarizer.write().overwrite().save(d)
            binarizer2 = Binarizer.load(d)
            self.assertEqual(str(binarizer), str(binarizer2))

    def test_quantile_discretizer_single_column(self):
        spark = self.spark
        values = [(0.1,), (0.4,), (1.2,), (1.5,), (float("nan"),), (float("nan"),)]
        df = spark.createDataFrame(values, ["values"])

        qds = QuantileDiscretizer(inputCol="values", outputCol="buckets")
        qds.setNumBuckets(2)
        qds.setRelativeError(0.01)
        qds.setHandleInvalid("keep")

        self.assertEqual(qds.getInputCol(), "values")
        self.assertEqual(qds.getOutputCol(), "buckets")
        self.assertEqual(qds.getNumBuckets(), 2)
        self.assertEqual(qds.getRelativeError(), 0.01)
        self.assertEqual(qds.getHandleInvalid(), "keep")

        bucketizer = qds.fit(df)
        self.assertIsInstance(bucketizer, Bucketizer)
        # Bucketizer doesn't inherit uid from QuantileDiscretizer
        self.assertNotEqual(qds.uid, bucketizer.uid)
        self.assertTrue(qds.uid.startswith("QuantileDiscretizer"))
        self.assertTrue(bucketizer.uid.startswith("Bucketizer"))

        # check model coefficients
        self.assertEqual(bucketizer.getSplits(), [float("-inf"), 0.4, float("inf")])

        output = bucketizer.transform(df)
        self.assertEqual(output.columns, ["values", "buckets"])
        self.assertEqual(output.count(), 6)

        # save & load
        with tempfile.TemporaryDirectory(prefix="quantile_discretizer_single_column") as d:
            qds.write().overwrite().save(d)
            qds2 = QuantileDiscretizer.load(d)
            self.assertEqual(str(qds), str(qds2))

            bucketizer.write().overwrite().save(d)
            bucketizer2 = Bucketizer.load(d)
            self.assertEqual(str(bucketizer), str(bucketizer2))

    def test_quantile_discretizer_multiple_columns(self):
        spark = self.spark
        inputs = [
            (0.1, 0.0),
            (0.4, 1.0),
            (1.2, 1.3),
            (1.5, 1.5),
            (float("nan"), float("nan")),
            (float("nan"), float("nan")),
        ]
        df = spark.createDataFrame(inputs, ["input1", "input2"])

        qds = QuantileDiscretizer(
            relativeError=0.01,
            handleInvalid="keep",
            numBuckets=2,
            inputCols=["input1", "input2"],
            outputCols=["output1", "output2"],
        )

        self.assertEqual(qds.getInputCols(), ["input1", "input2"])
        self.assertEqual(qds.getOutputCols(), ["output1", "output2"])
        self.assertEqual(qds.getNumBuckets(), 2)
        self.assertEqual(qds.getRelativeError(), 0.01)
        self.assertEqual(qds.getHandleInvalid(), "keep")

        bucketizer = qds.fit(df)
        self.assertIsInstance(bucketizer, Bucketizer)
        # Bucketizer doesn't inherit uid from QuantileDiscretizer
        self.assertNotEqual(qds.uid, bucketizer.uid)
        self.assertTrue(qds.uid.startswith("QuantileDiscretizer"))
        self.assertTrue(bucketizer.uid.startswith("Bucketizer"))

        # check model coefficients
        self.assertEqual(
            bucketizer.getSplitsArray(),
            [
                [float("-inf"), 0.4, float("inf")],
                [float("-inf"), 1.0, float("inf")],
            ],
        )

        output = bucketizer.transform(df)
        self.assertEqual(output.columns, ["input1", "input2", "output1", "output2"])
        self.assertEqual(output.count(), 6)

        # save & load
        with tempfile.TemporaryDirectory(prefix="quantile_discretizer_multiple_columns") as d:
            qds.write().overwrite().save(d)
            qds2 = QuantileDiscretizer.load(d)
            self.assertEqual(str(qds), str(qds2))

            bucketizer.write().overwrite().save(d)
            bucketizer2 = Bucketizer.load(d)
            self.assertEqual(str(bucketizer), str(bucketizer2))

    def test_bucketizer(self):
        df = self.spark.createDataFrame(
            [
                (0.1, 0.0),
                (0.4, 1.0),
                (1.2, 1.3),
                (1.5, float("nan")),
                (float("nan"), 1.0),
                (float("nan"), 0.0),
            ],
            ["v1", "v2"],
        )

        splits = [-float("inf"), 0.5, 1.4, float("inf")]
        bucketizer = Bucketizer()
        bucketizer.setSplits(splits)
        bucketizer.setHandleInvalid("keep")
        bucketizer.setInputCol("v1")
        bucketizer.setOutputCol("b1")

        self.assertEqual(bucketizer.getSplits(), splits)
        self.assertEqual(bucketizer.getHandleInvalid(), "keep")
        self.assertEqual(bucketizer.getInputCol(), "v1")
        self.assertEqual(bucketizer.getOutputCol(), "b1")

        output = bucketizer.transform(df)
        self.assertEqual(output.columns, ["v1", "v2", "b1"])
        self.assertEqual(output.count(), 6)
        self.assertEqual(
            [r.b1 for r in output.select("b1").collect()],
            [0.0, 0.0, 1.0, 2.0, 3.0, 3.0],
        )

        splitsArray = [
            [-float("inf"), 0.5, 1.4, float("inf")],
            [-float("inf"), 0.5, float("inf")],
        ]
        bucketizer = Bucketizer(
            splitsArray=splitsArray,
            inputCols=["v1", "v2"],
            outputCols=["b1", "b2"],
        )
        bucketizer.setHandleInvalid("keep")
        self.assertEqual(bucketizer.getSplitsArray(), splitsArray)
        self.assertEqual(bucketizer.getHandleInvalid(), "keep")
        self.assertEqual(bucketizer.getInputCols(), ["v1", "v2"])
        self.assertEqual(bucketizer.getOutputCols(), ["b1", "b2"])

        output = bucketizer.transform(df)
        self.assertEqual(output.columns, ["v1", "v2", "b1", "b2"])
        self.assertEqual(output.count(), 6)
        self.assertEqual(
            [r.b1 for r in output.select("b1").collect()],
            [0.0, 0.0, 1.0, 2.0, 3.0, 3.0],
        )
        self.assertEqual(
            [r.b2 for r in output.select("b2").collect()],
            [0.0, 1.0, 1.0, 2.0, 1.0, 0.0],
        )

        # save & load
        with tempfile.TemporaryDirectory(prefix="bucketizer") as d:
            bucketizer.write().overwrite().save(d)
            bucketizer2 = Bucketizer.load(d)
            self.assertEqual(str(bucketizer), str(bucketizer2))

    def test_idf(self):
        df = self.spark.createDataFrame(
            [
                (DenseVector([1.0, 2.0]),),
                (DenseVector([0.0, 1.0]),),
                (DenseVector([3.0, 0.2]),),
            ],
            ["tf"],
        )
        idf = IDF(inputCol="tf")
        self.assertListEqual(idf.params, [idf.inputCol, idf.minDocFreq, idf.outputCol])

        model = idf.fit(df, {idf.outputCol: "idf"})
        self.assertEqual(idf.uid, model.uid)
        # self.assertEqual(
        #     model.uid, idf.uid, "Model should inherit the UID from its parent estimator."
        # )
        self.assertTrue(
            np.allclose(model.idf.toArray(), [0.28768207245178085, 0.0], atol=1e-4),
            model.idf,
        )
        self.assertEqual(model.docFreq, [2, 3])
        self.assertEqual(model.numDocs, 3)

        output = model.transform(df)
        self.assertEqual(output.columns, ["tf", "idf"])
        self.assertIsNotNone(output.head().idf)

        # save & load
        with tempfile.TemporaryDirectory(prefix="idf") as d:
            idf.write().overwrite().save(d)
            idf2 = IDF.load(d)
            self.assertEqual(str(idf), str(idf2))

            model.write().overwrite().save(d)
            model2 = IDFModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_ngram(self):
        spark = self.spark
        df = spark.createDataFrame([Row(input=["a", "b", "c", "d", "e"])])

        ngram = NGram(n=4, inputCol="input", outputCol="output")
        self.assertEqual(ngram.getN(), 4)
        self.assertEqual(ngram.getInputCol(), "input")
        self.assertEqual(ngram.getOutputCol(), "output")

        output = ngram.transform(df)
        self.assertEqual(output.head().output, ["a b c d", "b c d e"])

        # save & load
        with tempfile.TemporaryDirectory(prefix="ngram") as d:
            ngram.write().overwrite().save(d)
            ngram2 = NGram.load(d)
            self.assertEqual(str(ngram), str(ngram2))

    def test_normalizer(self):
        spark = self.spark
        df = spark.createDataFrame(
            [(Vectors.dense([3.0, -4.0]),), (Vectors.sparse(4, {1: 4.0, 3: 3.0}),)],
            ["input"],
        )

        normalizer = Normalizer(p=2.0, inputCol="input", outputCol="output")
        self.assertEqual(normalizer.getP(), 2.0)
        self.assertEqual(normalizer.getInputCol(), "input")
        self.assertEqual(normalizer.getOutputCol(), "output")

        output = normalizer.transform(df)
        self.assertEqual(output.columns, ["input", "output"])
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="normalizer") as d:
            normalizer.write().overwrite().save(d)
            normalizer2 = Normalizer.load(d)
            self.assertEqual(str(normalizer), str(normalizer2))

    def test_interaction(self):
        spark = self.spark
        df = spark.createDataFrame([(0.0, 1.0), (2.0, 3.0)], ["a", "b"])

        interaction = Interaction()
        interaction.setInputCols(["a", "b"])
        interaction.setOutputCol("ab")
        self.assertEqual(interaction.getInputCols(), ["a", "b"])
        self.assertEqual(interaction.getOutputCol(), "ab")

        output = interaction.transform(df)
        self.assertEqual(output.columns, ["a", "b", "ab"])
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="interaction") as d:
            interaction.write().overwrite().save(d)
            interaction2 = Interaction.load(d)
            self.assertEqual(str(interaction), str(interaction2))

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
        self.assertEqual(cv.uid, model.uid)

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
        self.assertEqual(cv.uid, model1.uid)

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
        with self.assertRaisesRegex(Exception, "Vocabulary list cannot be empty"):
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
        self.assertEqual(rf.uid, model.uid)

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
        model = rf.fit(df)
        self.assertEqual(rf.uid, model.uid)
        transformedDF = model.transform(df)
        observed = transformedDF.select("features").collect()
        expected = [[1.0, 0.0], [2.0, 1.0], [0.0, 0.0]]
        for i in range(0, len(expected)):
            self.assertTrue(all(observed[i]["features"].toArray() == expected[i]))

        # save & load
        with tempfile.TemporaryDirectory(prefix="rformula") as d:
            rf.write().overwrite().save(d)
            rf2 = RFormula.load(d)
            self.assertEqual(str(rf), str(rf2))

            model.write().overwrite().save(d)
            model2 = RFormulaModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.getFormula(), model2.getFormula())

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
        self.assertEqual(model.labelsArray, [["a", "b", "c"]])

        self.assertEqual(model.getInputCol(), "label")
        self.assertEqual(model.getOutputCol(), "indexed")
        self.assertEqual(model.getHandleInvalid(), "keep")

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

    def test_string_indexer_from_arrays_of_labels(self):
        model = StringIndexerModel.from_arrays_of_labels(
            [["a", "b", "c"], ["x", "y", "z"]],
            inputCols=["label1", "label2"],
            outputCols=["indexed1", "indexed2"],
            handleInvalid="keep",
        )

        self.assertEqual(model.labelsArray, [["a", "b", "c"], ["x", "y", "z"]])

        self.assertEqual(model.getInputCols(), ["label1", "label2"])
        self.assertEqual(model.getOutputCols(), ["indexed1", "indexed2"])
        self.assertEqual(model.getHandleInvalid(), "keep")

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
        output = model.transform(df)
        self.assertEqual(
            output.columns,
            ["input1", "input2", "input3", "label", "output", "output2", "output3"],
        )
        self.assertEqual(output.count(), 9)

        # save & load
        with tempfile.TemporaryDirectory(prefix="target_encoder") as d:
            encoder.write().overwrite().save(d)
            encoder2 = TargetEncoder.load(d)
            self.assertEqual(str(encoder), str(encoder2))

            model.write().overwrite().save(d)
            model2 = TargetEncoderModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_vector_size_hint(self):
        df = self.spark.createDataFrame(
            [
                (0, Vectors.dense([0.0, 10.0, 0.5])),
                (1, Vectors.dense([1.0, 11.0, 0.5, 0.6])),
                (2, Vectors.dense([2.0, 12.0])),
            ],
            ["id", "vector"],
        )

        sh = VectorSizeHint(inputCol="vector", handleInvalid="skip")
        sh.setSize(3)
        self.assertEqual(sh.getSize(), 3)

        output = sh.transform(df).head().vector
        expected = DenseVector([0.0, 10.0, 0.5])
        self.assertEqual(output, expected)

        # save & load
        with tempfile.TemporaryDirectory(prefix="vector_size_hint") as d:
            sh.write().overwrite().save(d)
            sh2 = VectorSizeHint.load(d)
            self.assertEqual(str(sh), str(sh2))

    def test_vector_slicer(self):
        spark = self.spark

        df = spark.createDataFrame(
            [
                (Vectors.dense([-2.0, 2.3, 0.0, 0.0, 1.0]),),
                (Vectors.dense([0.0, 0.0, 0.0, 0.0, 0.0]),),
                (Vectors.dense([0.6, -1.1, -3.0, 4.5, 3.3]),),
            ],
            ["features"],
        )

        vs = VectorSlicer(outputCol="sliced", indices=[1, 4])
        vs.setInputCol("features")
        self.assertEqual(vs.getIndices(), [1, 4])
        self.assertEqual(vs.getInputCol(), "features")
        self.assertEqual(vs.getOutputCol(), "sliced")

        output = vs.transform(df)
        self.assertEqual(output.columns, ["features", "sliced"])
        self.assertEqual(output.count(), 3)
        self.assertEqual(output.head().sliced, Vectors.dense([2.3, 1.0]))

        # save & load
        with tempfile.TemporaryDirectory(prefix="vector_slicer") as d:
            vs.write().overwrite().save(d)
            vs2 = VectorSlicer.load(d)
            self.assertEqual(str(vs), str(vs2))

    def test_feature_hasher(self):
        data = [(2.0, True, "1", "foo"), (3.0, False, "2", "bar")]
        cols = ["real", "bool", "stringNum", "string"]
        df = self.spark.createDataFrame(data, cols)

        hasher = FeatureHasher(numFeatures=2)
        hasher.setInputCols(cols)
        hasher.setOutputCol("features")

        self.assertEqual(hasher.getNumFeatures(), 2)
        self.assertEqual(hasher.getInputCols(), cols)
        self.assertEqual(hasher.getOutputCol(), "features")

        output = hasher.transform(df)
        self.assertEqual(output.columns, ["real", "bool", "stringNum", "string", "features"])
        self.assertEqual(output.count(), 2)

        features = output.head().features.toArray()
        self.assertTrue(
            np.allclose(features, [2.0, 3.0], atol=1e-4),
            features,
        )

        # save & load
        with tempfile.TemporaryDirectory(prefix="feature_hasher") as d:
            hasher.write().overwrite().save(d)
            hasher2 = FeatureHasher.load(d)
            self.assertEqual(str(hasher), str(hasher2))

    def test_hashing_tf(self):
        df = self.spark.createDataFrame([(0, ["a", "a", "b", "c", "c", "c"])], ["id", "words"])
        tf = HashingTF()
        tf.setInputCol("words").setOutputCol("features").setNumFeatures(10).setBinary(True)
        self.assertEqual(tf.getInputCol(), "words")
        self.assertEqual(tf.getOutputCol(), "features")
        self.assertEqual(tf.getNumFeatures(), 10)
        self.assertTrue(tf.getBinary())

        output = tf.transform(df)
        self.assertEqual(output.columns, ["id", "words", "features"])
        self.assertEqual(output.count(), 1)

        features = output.select("features").first().features.toArray()
        self.assertTrue(
            np.allclose(
                features,
                [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0],
                atol=1e-4,
            ),
            features,
        )

        # save & load
        with tempfile.TemporaryDirectory(prefix="hashing_tf") as d:
            tf.write().overwrite().save(d)
            tf2 = HashingTF.load(d)
            self.assertEqual(str(tf), str(tf2))

    def test_bucketed_random_projection_lsh(self):
        spark = self.spark

        data = [
            (0, Vectors.dense([-1.0, -1.0])),
            (1, Vectors.dense([-1.0, 1.0])),
            (2, Vectors.dense([1.0, -1.0])),
            (3, Vectors.dense([1.0, 1.0])),
        ]
        df = spark.createDataFrame(data, ["id", "features"])

        data2 = [
            (4, Vectors.dense([2.0, 2.0])),
            (5, Vectors.dense([2.0, 3.0])),
            (6, Vectors.dense([3.0, 2.0])),
            (7, Vectors.dense([3.0, 3.0])),
        ]
        df2 = spark.createDataFrame(data2, ["id", "features"])

        brp = BucketedRandomProjectionLSH()
        brp.setInputCol("features")
        brp.setOutputCol("hashes")
        brp.setSeed(12345)
        brp.setBucketLength(1.0)

        self.assertEqual(brp.getInputCol(), "features")
        self.assertEqual(brp.getOutputCol(), "hashes")
        self.assertEqual(brp.getBucketLength(), 1.0)
        self.assertEqual(brp.getSeed(), 12345)

        model = brp.fit(df)

        output = model.transform(df)
        self.assertEqual(output.columns, ["id", "features", "hashes"])
        self.assertEqual(output.count(), 4)

        output = model.approxNearestNeighbors(df2, Vectors.dense([1.0, 2.0]), 1)
        self.assertEqual(output.columns, ["id", "features", "hashes", "distCol"])
        self.assertEqual(output.count(), 1)

        output = model.approxSimilarityJoin(df, df2, 3)
        self.assertEqual(output.columns, ["datasetA", "datasetB", "distCol"])
        self.assertEqual(output.count(), 1)

        # save & load
        with tempfile.TemporaryDirectory(prefix="bucketed_random_projection_lsh") as d:
            brp.write().overwrite().save(d)
            brp2 = BucketedRandomProjectionLSH.load(d)
            self.assertEqual(str(brp), str(brp2))

            model.write().overwrite().save(d)
            model2 = BucketedRandomProjectionLSHModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_min_hash_lsh(self):
        spark = self.spark

        data = [
            (0, Vectors.dense([-1.0, -1.0])),
            (1, Vectors.dense([-1.0, 1.0])),
            (2, Vectors.dense([1.0, -1.0])),
            (3, Vectors.dense([1.0, 1.0])),
        ]
        df = spark.createDataFrame(data, ["id", "features"])

        data2 = [
            (4, Vectors.dense([2.0, 2.0])),
            (5, Vectors.dense([2.0, 3.0])),
            (6, Vectors.dense([3.0, 2.0])),
            (7, Vectors.dense([3.0, 3.0])),
        ]
        df2 = spark.createDataFrame(data2, ["id", "features"])

        mh = MinHashLSH()
        mh.setInputCol("features")
        mh.setOutputCol("hashes")
        mh.setSeed(12345)

        self.assertEqual(mh.getInputCol(), "features")
        self.assertEqual(mh.getOutputCol(), "hashes")
        self.assertEqual(mh.getSeed(), 12345)

        model = mh.fit(df)

        output = model.transform(df)
        self.assertEqual(output.columns, ["id", "features", "hashes"])
        self.assertEqual(output.count(), 4)

        output = model.approxNearestNeighbors(df2, Vectors.dense([1.0, 2.0]), 1)
        self.assertEqual(output.columns, ["id", "features", "hashes", "distCol"])
        self.assertEqual(output.count(), 1)

        output = model.approxSimilarityJoin(df, df2, 3)
        self.assertEqual(output.columns, ["datasetA", "datasetB", "distCol"])
        self.assertEqual(output.count(), 16)

        # save & load
        with tempfile.TemporaryDirectory(prefix="min_hash_lsh") as d:
            mh.write().overwrite().save(d)
            mh2 = MinHashLSH.load(d)
            self.assertEqual(str(mh), str(mh2))

            model.write().overwrite().save(d)
            model2 = MinHashLSHModel.load(d)
            self.assertEqual(str(model), str(model2))


class FeatureTests(FeatureTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_feature import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
