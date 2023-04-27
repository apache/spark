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
import numpy as np
import pandas as pd
import unittest

from pyspark.ml.functions import predict_batch_udf
from pyspark.sql.functions import array, struct, col
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StructType, StructField, FloatType
from pyspark.testing.mlutils import SparkSessionTestCase


class PredictBatchUDFTests(SparkSessionTestCase):
    def setUp(self):
        super(PredictBatchUDFTests, self).setUp()
        self.data = np.arange(0, 1000, dtype=np.float64).reshape(-1, 4)

        # 4 scalar columns
        self.pdf = pd.DataFrame(self.data, columns=["a", "b", "c", "d"])
        self.df = self.spark.createDataFrame(self.pdf)

        # 1 tensor column of 4 doubles
        self.pdf_tensor = pd.DataFrame()
        self.pdf_tensor["t1"] = self.pdf.values.tolist()
        self.df_tensor1 = self.spark.createDataFrame(self.pdf_tensor)

        # 2 tensor columns of 4 doubles and 3 doubles
        self.pdf_tensor["t2"] = self.pdf.drop(columns="d").values.tolist()
        self.df_tensor2 = self.spark.createDataFrame(self.pdf_tensor)

        # 4 scalar columns with 1 tensor column
        self.pdf_scalar_tensor = self.pdf
        self.pdf_scalar_tensor["t1"] = self.pdf.values.tolist()
        self.df_scalar_tensor = self.spark.createDataFrame(self.pdf_scalar_tensor)

    def test_identity_single(self):
        def make_predict_fn():
            def predict(inputs):
                return inputs

            return predict

        identity = predict_batch_udf(make_predict_fn, return_type=DoubleType(), batch_size=5)

        # single column input => single column output (struct)
        preds = self.df.withColumn("preds", identity(struct("a"))).toPandas()
        self.assertTrue(preds["a"].equals(preds["preds"]))

        # single column input => single column output (col)
        preds = self.df.withColumn("preds", identity(col("a"))).toPandas()
        self.assertTrue(preds["a"].equals(preds["preds"]))

        # single column input => single column output (str)
        preds = self.df.withColumn("preds", identity("a")).toPandas()
        self.assertTrue(preds["a"].equals(preds["preds"]))

        # multiple column input, single input => ERROR
        with self.assertRaisesRegex(Exception, "Multiple input columns found, but model expected"):
            preds = self.df.withColumn("preds", identity("a", "b")).toPandas()

    def test_identity_multi(self):
        # single input model
        def make_predict_fn():
            def predict(inputs):
                return {"a1": inputs[:, 0], "b1": inputs[:, 1]}

            return predict

        identity = predict_batch_udf(
            make_predict_fn,
            return_type=StructType(
                [StructField("a1", DoubleType(), True), StructField("b1", DoubleType(), True)]
            ),
            batch_size=5,
        )

        # multiple columns using struct, single input => multiple column output
        preds = (
            self.df.withColumn("preds", identity(struct("a", "b")))
            .select("a", "b", "preds.*")
            .toPandas()
        )
        self.assertTrue(preds["a"].equals(preds["a1"]))
        self.assertTrue(preds["b"].equals(preds["b1"]))

        # multiple columns, single input => ERROR
        with self.assertRaisesRegex(Exception, "Multiple input columns found, but model expected"):
            preds = (
                self.df.withColumn("preds", identity("a", "b"))
                .select("a", "b", "preds.*")
                .toPandas()
            )

        # multiple input model
        def predict_batch2_fn():
            def predict(in1, in2):
                return {"a1": in1, "b1": in2}

            return predict

        identity2 = predict_batch_udf(
            predict_batch2_fn,
            return_type=StructType(
                [StructField("a1", DoubleType(), True), StructField("b1", DoubleType(), True)]
            ),
            batch_size=5,
        )

        # multiple columns using struct, multiple inputs => multiple column output
        preds = (
            self.df.withColumn("preds", identity2(struct("a", "b")))
            .select("a", "b", "preds.*")
            .toPandas()
        )
        self.assertTrue(preds["a"].equals(preds["a1"]))
        self.assertTrue(preds["b"].equals(preds["b1"]))

        # multiple columns, multiple inputs => multiple column output
        preds = (
            self.df.withColumn("preds", identity2(col("a"), col("b")))
            .select("a", "b", "preds.*")
            .toPandas()
        )
        self.assertTrue(preds["a"].equals(preds["a1"]))
        self.assertTrue(preds["b"].equals(preds["b1"]))

        # multiple column input => multiple column output (str)
        preds = (
            self.df.withColumn("preds", identity2("a", "b")).select("a", "b", "preds.*").toPandas()
        )
        self.assertTrue(preds["a"].equals(preds["a1"]))
        self.assertTrue(preds["b"].equals(preds["b1"]))

    def test_batching(self):
        batch_size = 10

        def make_predict_fn():
            def predict(inputs):
                batch_size = len(inputs)
                # just return the batch size as the "prediction"
                outputs = [batch_size for i in inputs]
                return np.array(outputs)

            return predict

        identity = predict_batch_udf(
            make_predict_fn, return_type=IntegerType(), batch_size=batch_size
        )

        # struct
        preds = self.df.withColumn("preds", identity(struct("a"))).toPandas()
        batch_sizes = preds["preds"].to_numpy()
        self.assertTrue(all(batch_sizes <= batch_size))

        # col
        preds = self.df.withColumn("preds", identity(col("a"))).toPandas()
        batch_sizes = preds["preds"].to_numpy()
        self.assertTrue(all(batch_sizes <= batch_size))

        # struct
        preds = self.df.withColumn("preds", identity("a")).toPandas()
        batch_sizes = preds["preds"].to_numpy()
        self.assertTrue(all(batch_sizes <= batch_size))

    def test_caching(self):
        def make_predict_fn():
            # emulate loading a model, this should only be invoked once (per worker process)
            fake_output = np.random.random()

            def predict(inputs):
                return np.array([fake_output for i in inputs])

            return predict

        identity = predict_batch_udf(make_predict_fn, return_type=DoubleType(), batch_size=5)

        # results should be the same
        df1 = self.df.withColumn("preds", identity(struct("a"))).toPandas()
        df2 = self.df.withColumn("preds", identity(struct("a"))).toPandas()
        self.assertTrue(df1.equals(df2))

        identity = predict_batch_udf(make_predict_fn, return_type=DoubleType(), batch_size=5)

        # cache should now be invalidated and results should be different
        df3 = self.df.withColumn("preds", identity(struct("a"))).toPandas()
        self.assertFalse(df1.equals(df3))

    def test_transform_scalar(self):
        columns = self.df.columns

        # multiple scalar columns, single input, no input_tensor_shapes => single numpy array
        def array_sum_fn():
            def predict(inputs):
                return np.sum(inputs, axis=1)

            return predict

        sum_cols = predict_batch_udf(array_sum_fn, return_type=DoubleType(), batch_size=5)
        preds = self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        with self.assertRaisesRegex(Exception, "Multiple input columns found, but model expected"):
            preds = self.df.withColumn("preds", sum_cols(*[col(c) for c in columns])).toPandas()

        with self.assertRaisesRegex(Exception, "Multiple input columns found, but model expected"):
            preds = self.df.withColumn("preds", sum_cols(*columns)).toPandas()

        # multiple scalar columns, multiple inputs, no input_tensor_shapes => list of numpy arrays
        def list_sum_fn():
            def predict(a, b, c, d):
                result = sum([a, b, c, d])
                return result

            return predict

        sum_cols = predict_batch_udf(list_sum_fn, return_type=DoubleType(), batch_size=5)
        preds = self.df.withColumn("preds", sum_cols(*columns)).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # multiple scalar columns, mismatched inputs, no input_tensor_shapes  => ERROR
        def list_sum_fn():
            def predict(a, b, c):
                result = sum([a, b, c])
                return result

            return predict

        sum_cols = predict_batch_udf(list_sum_fn, return_type=DoubleType(), batch_size=5)
        with self.assertRaisesRegex(Exception, "Model expected 3 inputs, but received 4 columns"):
            preds = self.df.withColumn("preds", sum_cols(*columns)).toPandas()

        # muliple scalar columns with one tensor_input_shape => single numpy array
        sum_cols = predict_batch_udf(
            array_sum_fn, return_type=DoubleType(), batch_size=5, input_tensor_shapes=[[4]]
        )
        preds = self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # muliple scalar columns with wrong tensor_input_shape => ERROR
        sum_cols = predict_batch_udf(
            array_sum_fn, return_type=DoubleType(), batch_size=5, input_tensor_shapes=[[3]]
        )
        with self.assertRaisesRegex(Exception, "Input data does not match expected shape."):
            self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()

        # scalar columns with multiple tensor_input_shapes => ERROR
        sum_cols = predict_batch_udf(
            array_sum_fn,
            return_type=DoubleType(),
            batch_size=5,
            input_tensor_shapes=[[4], [4]],
        )
        with self.assertRaisesRegex(Exception, "Multiple input_tensor_shapes found"):
            self.df.withColumn("preds", sum_cols(struct(*columns))).toPandas()

    def test_transform_single_tensor(self):
        columns1 = self.df_tensor1.columns

        def array_sum_fn():
            def predict(inputs):
                return np.sum(inputs, axis=1)

            return predict

        # tensor column with no input_tensor_shapes => ERROR
        sum_cols = predict_batch_udf(array_sum_fn, return_type=DoubleType(), batch_size=5)
        with self.assertRaisesRegex(Exception, "Tensor columns require input_tensor_shapes"):
            preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns1))).toPandas()

        # tensor column with tensor_input_shapes => single numpy array
        sum_cols = predict_batch_udf(
            array_sum_fn, return_type=DoubleType(), batch_size=5, input_tensor_shapes=[[4]]
        )
        preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns1))).toPandas()
        self.assertTrue(np.array_equal(np.sum(self.data, axis=1), preds["preds"].to_numpy()))

        # tensor column with multiple tensor_input_shapes => ERROR
        sum_cols = predict_batch_udf(
            array_sum_fn,
            return_type=DoubleType(),
            batch_size=5,
            input_tensor_shapes=[[4], [3]],
        )
        with self.assertRaisesRegex(Exception, "Multiple input_tensor_shapes found"):
            preds = self.df_tensor1.withColumn("preds", sum_cols(struct(*columns1))).toPandas()

    def test_transform_multi_tensor(self):
        def multi_sum_fn():
            def predict(t1, t2):
                result = np.sum(t1, axis=1) + np.sum(t2, axis=1)
                return result

            return predict

        # multiple tensor columns with tensor_input_shapes => list of numpy arrays
        sum_cols = predict_batch_udf(
            multi_sum_fn,
            return_type=DoubleType(),
            batch_size=5,
            input_tensor_shapes=[[4], [3]],
        )
        preds = self.df_tensor2.withColumn("preds", sum_cols("t1", "t2")).toPandas()
        self.assertTrue(
            np.array_equal(
                np.sum(self.data, axis=1) + np.sum(self.data[:, 0:3], axis=1),
                preds["preds"].to_numpy(),
            )
        )

    def test_mixed_input_shapes(self):
        def mixed_sum_fn():
            # 4 scalars + 1 tensor
            def predict(a, b, c, d, t1):
                result = a + b + c + d + np.sum(t1, axis=1)
                return result

            return predict

        # dense input_tensor_shapes
        sum_cols = predict_batch_udf(
            mixed_sum_fn,
            return_type=DoubleType(),
            batch_size=5,
            input_tensor_shapes=[None, None, None, None, [4]],
        )

        preds = self.df_scalar_tensor.withColumn(
            "preds", sum_cols("a", "b", "c", "d", "t1")
        ).toPandas()

        self.assertTrue(
            np.array_equal(
                np.sum(self.data, axis=1) * 2,
                preds["preds"].to_numpy(),
            )
        )

        # sparse input_tensor_shapes
        sum_cols = predict_batch_udf(
            mixed_sum_fn,
            return_type=DoubleType(),
            batch_size=5,
            input_tensor_shapes={4: [4]},
        )

        preds = self.df_scalar_tensor.withColumn(
            "preds", sum_cols("a", "b", "c", "d", "t1")
        ).toPandas()

        self.assertTrue(
            np.array_equal(
                np.sum(self.data, axis=1) * 2,
                preds["preds"].to_numpy(),
            )
        )

    def test_return_multiple(self):
        # columnar form (dictionary of numpy arrays)
        def multiples_column_fn():
            def predict(inputs):
                return {"x2": inputs * 2, "x3": inputs * 3}

            return predict

        multiples_col = predict_batch_udf(
            multiples_column_fn,
            return_type=StructType(
                [StructField("x2", DoubleType(), True), StructField("x3", DoubleType(), True)]
            ),
            batch_size=5,
        )
        preds = self.df.withColumn("preds", multiples_col("a")).select("a", "preds.*").toPandas()

        self.assertTrue(np.array_equal(self.data[:, 0] * 2, preds["x2"].to_numpy()))
        self.assertTrue(np.array_equal(self.data[:, 0] * 3, preds["x3"].to_numpy()))

        # row form: list of dictionaries
        def multiples_row_fn():
            def predict(inputs):
                return [{"x2": x * 2, "x3": x * 3} for x in inputs]

            return predict

        multiples_row = predict_batch_udf(
            multiples_row_fn,
            return_type=StructType(
                [StructField("x2", DoubleType(), True), StructField("x3", DoubleType(), True)]
            ),
            batch_size=5,
        )
        preds = self.df.withColumn("preds", multiples_row("a")).select("a", "preds.*").toPandas()

        self.assertTrue(np.array_equal(self.data[:, 0] * 2, preds["x2"].to_numpy()))
        self.assertTrue(np.array_equal(self.data[:, 0] * 3, preds["x3"].to_numpy()))

    def test_return_struct_with_array_field(self):
        # column form
        def multiples_with_array_fn():
            def predict(x, y):
                return {"x2": x * 2, "y3": y * 3}

            return predict

        multiples_w_array = predict_batch_udf(
            multiples_with_array_fn,
            return_type=StructType(
                [
                    StructField("x2", DoubleType(), True),
                    StructField("y3", ArrayType(DoubleType()), True),
                ]
            ),
            input_tensor_shapes=[[], [3]],
            batch_size=5,
        )
        preds = (
            self.df.withColumn("preds", multiples_w_array("a", array(["b", "c", "d"])))
            .select("a", "preds.*")
            .toPandas()
        )

        self.assertTrue(np.array_equal(self.data[:, 0] * 2, np.array(preds["x2"])))
        self.assertTrue(np.array_equal(self.data[:, 1:4] * 3, np.vstack(preds["y3"])))

        # row form: list of dictionaries
        def multiples_row_array_fn():
            def predict(x, y):
                return [{"x2": x * 2, "y3": y * 3} for x, y in zip(x, y)]

            return predict

        multiples_row_array = predict_batch_udf(
            multiples_row_array_fn,
            return_type=StructType(
                [
                    StructField("x2", DoubleType(), True),
                    StructField("y3", ArrayType(DoubleType()), True),
                ]
            ),
            input_tensor_shapes=[[], [3]],
            batch_size=5,
        )

        preds = (
            self.df.withColumn("preds", multiples_row_array("a", array(["b", "c", "d"])))
            .select("a", "preds.*")
            .toPandas()
        )

        self.assertTrue(np.array_equal(self.data[:, 0] * 2, np.array(preds["x2"])))
        self.assertTrue(np.array_equal(self.data[:, 1:4] * 3, np.vstack(preds["y3"])))

        # row form: list of dictionaries, malformed array
        def multiples_row_array_fn():
            def predict(x, y):
                return [{"x2": x * 2, "y3": np.reshape(y, (-1, 1)) * 3} for x, y in zip(x, y)]

            return predict

        multiples_row_array = predict_batch_udf(
            multiples_row_array_fn,
            return_type=StructType(
                [
                    StructField("x2", DoubleType(), True),
                    StructField("y3", ArrayType(DoubleType()), True),
                ]
            ),
            input_tensor_shapes=[[], [3]],
            batch_size=5,
        )
        with self.assertRaisesRegex(Exception, "must be one-dimensional"):
            preds = (
                self.df.withColumn("preds", multiples_row_array("a", array(["b", "c", "d"])))
                .select("a", "preds.*")
                .toPandas()
            )

    def test_single_value_in_batch(self):
        # SPARK-42250: batches consisting of single float value should work
        df = self.spark.createDataFrame(
            [[[0.0, 1.0, 2.0, 3.0], [0.0, 1.0, 2.0]]], schema=["t1", "t2"]
        )

        def make_multi_sum_fn():
            def predict(x1: np.ndarray, x2: np.ndarray) -> np.ndarray:
                return np.sum(x1, axis=1) + np.sum(x2, axis=1)

            return predict

        multi_sum_udf = predict_batch_udf(
            make_multi_sum_fn,
            return_type=FloatType(),
            batch_size=1,
            input_tensor_shapes=[[4], [3]],
        )

        [value] = df.select(multi_sum_udf("t1", "t2")).first()
        self.assertEqual(value, 9.0)


if __name__ == "__main__":
    from pyspark.ml.tests.test_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
