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
import glob
import os

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class DataFrameSparkIOTest(PandasOnSparkTestCase, TestUtils):
    """Test cases for big data I/O using Spark."""

    @property
    def test_column_order(self):
        return ["i32", "i64", "f", "bhello"]

    @property
    def test_pdf(self):
        pdf = pd.DataFrame(
            {
                "i32": np.arange(20, dtype=np.int32) % 3,
                "i64": np.arange(20, dtype=np.int64) % 5,
                "f": np.arange(20, dtype=np.float64),
                "bhello": np.random.choice(["hello", "yo", "people"], size=20).astype("O"),
            },
            columns=self.test_column_order,
            index=np.random.rand(20),
        )
        return pdf

    def test_parquet_read(self):
        with self.temp_dir() as tmp:
            data = self.test_pdf
            self.spark.createDataFrame(data, "i32 int, i64 long, f double, bhello string").coalesce(
                1
            ).write.parquet(tmp, mode="overwrite")

            def check(columns):
                expected = pd.read_parquet(tmp, columns=columns)
                actual = ps.read_parquet(tmp, columns=columns)
                self.assertPandasEqual(expected, actual.to_pandas())

            check(None)
            check(["i32", "i64"])
            check(["i64", "i32"])

            # check with pyspark patch.
            expected = pd.read_parquet(tmp)
            actual = ps.read_parquet(tmp)
            self.assertPandasEqual(expected, actual.to_pandas())

            # When index columns are known
            pdf = self.test_pdf
            expected = ps.DataFrame(pdf)

            expected_idx = expected.set_index("bhello")[["f", "i32", "i64"]]
            actual_idx = ps.read_parquet(tmp, index_col="bhello")[["f", "i32", "i64"]]
            self.assert_eq(
                actual_idx.sort_values(by="f").to_spark().toPandas(),
                expected_idx.sort_values(by="f").to_spark().toPandas(),
            )

    def test_parquet_read_with_pandas_metadata(self):
        with self.temp_dir() as tmp:
            expected1 = self.test_pdf

            path1 = "{}/file1.parquet".format(tmp)
            expected1.to_parquet(path1)

            self.assert_eq(ps.read_parquet(path1, pandas_metadata=True), expected1)

            expected2 = expected1.reset_index()

            path2 = "{}/file2.parquet".format(tmp)
            expected2.to_parquet(path2)

            self.assert_eq(ps.read_parquet(path2, pandas_metadata=True), expected2)

            expected3 = expected2.set_index("index", append=True)

            path3 = "{}/file3.parquet".format(tmp)
            expected3.to_parquet(path3)

            self.assert_eq(ps.read_parquet(path3, pandas_metadata=True), expected3)

    def test_parquet_write(self):
        with self.temp_dir() as tmp:
            pdf = self.test_pdf
            expected = ps.DataFrame(pdf)

            # Write out partitioned by one column
            expected.to_parquet(tmp, mode="overwrite", partition_cols="i32")
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_parquet(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Write out partitioned by two columns
            expected.to_parquet(tmp, mode="overwrite", partition_cols=["i32", "bhello"])
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_parquet(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Set `compression` with string
            expected.to_parquet(tmp, mode="overwrite", partition_cols="i32", compression="none")
            actual = ps.read_parquet(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Test `options` parameter
            expected.to_parquet(
                tmp, mode="overwrite", partition_cols="i32", options={"compression": "none"}
            )
            actual = ps.read_parquet(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

    def test_table(self):
        with self.table("test_table"):
            pdf = self.test_pdf
            expected = ps.DataFrame(pdf)

            # Write out partitioned by one column
            expected.spark.to_table("test_table", mode="overwrite", partition_cols="i32")
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_table("test_table")
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Write out partitioned by two columns
            expected.to_table("test_table", mode="overwrite", partition_cols=["i32", "bhello"])
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_table("test_table")
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # When index columns are known
            expected_idx = expected.set_index("bhello")[["f", "i32", "i64"]]
            actual_idx = ps.read_table("test_table", index_col="bhello")[["f", "i32", "i64"]]
            self.assert_eq(
                actual_idx.sort_values(by="f").to_spark().toPandas(),
                expected_idx.sort_values(by="f").to_spark().toPandas(),
            )

            expected_idx = expected.set_index(["bhello"])[["f", "i32", "i64"]]
            actual_idx = ps.read_table("test_table", index_col=["bhello"])[["f", "i32", "i64"]]
            self.assert_eq(
                actual_idx.sort_values(by="f").to_spark().toPandas(),
                expected_idx.sort_values(by="f").to_spark().toPandas(),
            )

            expected_idx = expected.set_index(["i32", "bhello"])[["f", "i64"]]
            actual_idx = ps.read_table("test_table", index_col=["i32", "bhello"])[["f", "i64"]]
            self.assert_eq(
                actual_idx.sort_values(by="f").to_spark().toPandas(),
                expected_idx.sort_values(by="f").to_spark().toPandas(),
            )

    def test_spark_io(self):
        with self.temp_dir() as tmp:
            pdf = self.test_pdf
            expected = ps.DataFrame(pdf)

            # Write out partitioned by one column
            expected.to_spark_io(tmp, format="json", mode="overwrite", partition_cols="i32")
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_spark_io(tmp, format="json")
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Write out partitioned by two columns
            expected.to_spark_io(
                tmp, format="json", mode="overwrite", partition_cols=["i32", "bhello"]
            )
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_spark_io(path=tmp, format="json")
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # When index columns are known
            pdf = self.test_pdf
            expected = ps.DataFrame(pdf)
            col_order = ["f", "i32", "i64"]

            expected_idx = expected.set_index("bhello")[col_order]
            actual_idx = ps.read_spark_io(tmp, format="json", index_col="bhello")[col_order]
            self.assert_eq(
                actual_idx.sort_values(by="f").to_spark().toPandas(),
                expected_idx.sort_values(by="f").to_spark().toPandas(),
            )

    @unittest.skip("openpyxl")
    def test_read_excel(self):
        with self.temp_dir() as tmp:

            path1 = "{}/file1.xlsx".format(tmp)
            self.test_pdf[["i32"]].to_excel(path1)

            self.assert_eq(ps.read_excel(open(path1, "rb")), pd.read_excel(open(path1, "rb")))
            self.assert_eq(
                ps.read_excel(open(path1, "rb"), index_col=0),
                pd.read_excel(open(path1, "rb"), index_col=0),
            )
            self.assert_eq(
                ps.read_excel(open(path1, "rb"), index_col=0, squeeze=True),
                pd.read_excel(open(path1, "rb"), index_col=0, squeeze=True),
            )

            self.assert_eq(ps.read_excel(path1), pd.read_excel(path1))
            self.assert_eq(ps.read_excel(path1, index_col=0), pd.read_excel(path1, index_col=0))
            self.assert_eq(
                ps.read_excel(path1, index_col=0, squeeze=True),
                pd.read_excel(path1, index_col=0, squeeze=True),
            )

            self.assert_eq(ps.read_excel(tmp), pd.read_excel(path1))

            path2 = "{}/file2.xlsx".format(tmp)
            self.test_pdf[["i32"]].to_excel(path2)
            self.assert_eq(
                ps.read_excel(tmp, index_col=0).sort_index(),
                pd.concat(
                    [pd.read_excel(path1, index_col=0), pd.read_excel(path2, index_col=0)]
                ).sort_index(),
            )
            self.assert_eq(
                ps.read_excel(tmp, index_col=0, squeeze=True).sort_index(),
                pd.concat(
                    [
                        pd.read_excel(path1, index_col=0, squeeze=True),
                        pd.read_excel(path2, index_col=0, squeeze=True),
                    ]
                ).sort_index(),
            )

        with self.temp_dir() as tmp:
            path1 = "{}/file1.xlsx".format(tmp)
            with pd.ExcelWriter(path1) as writer:
                self.test_pdf.to_excel(writer, sheet_name="Sheet_name_1")
                self.test_pdf[["i32"]].to_excel(writer, sheet_name="Sheet_name_2")

            sheet_names = [["Sheet_name_1", "Sheet_name_2"], None]

            pdfs1 = pd.read_excel(open(path1, "rb"), sheet_name=None, index_col=0)
            pdfs1_squeezed = pd.read_excel(
                open(path1, "rb"), sheet_name=None, index_col=0, squeeze=True
            )

            for sheet_name in sheet_names:
                psdfs = ps.read_excel(open(path1, "rb"), sheet_name=sheet_name, index_col=0)
                self.assert_eq(psdfs["Sheet_name_1"], pdfs1["Sheet_name_1"])
                self.assert_eq(psdfs["Sheet_name_2"], pdfs1["Sheet_name_2"])

                psdfs = ps.read_excel(
                    open(path1, "rb"), sheet_name=sheet_name, index_col=0, squeeze=True
                )
                self.assert_eq(psdfs["Sheet_name_1"], pdfs1_squeezed["Sheet_name_1"])
                self.assert_eq(psdfs["Sheet_name_2"], pdfs1_squeezed["Sheet_name_2"])

            self.assert_eq(
                ps.read_excel(tmp, index_col=0, sheet_name="Sheet_name_2"),
                pdfs1["Sheet_name_2"],
            )

            for sheet_name in sheet_names:
                psdfs = ps.read_excel(tmp, sheet_name=sheet_name, index_col=0)
                self.assert_eq(psdfs["Sheet_name_1"], pdfs1["Sheet_name_1"])
                self.assert_eq(psdfs["Sheet_name_2"], pdfs1["Sheet_name_2"])

                psdfs = ps.read_excel(tmp, sheet_name=sheet_name, index_col=0, squeeze=True)
                self.assert_eq(psdfs["Sheet_name_1"], pdfs1_squeezed["Sheet_name_1"])
                self.assert_eq(psdfs["Sheet_name_2"], pdfs1_squeezed["Sheet_name_2"])

            path2 = "{}/file2.xlsx".format(tmp)
            with pd.ExcelWriter(path2) as writer:
                self.test_pdf.to_excel(writer, sheet_name="Sheet_name_1")
                self.test_pdf[["i32"]].to_excel(writer, sheet_name="Sheet_name_2")

            pdfs2 = pd.read_excel(path2, sheet_name=None, index_col=0)
            pdfs2_squeezed = pd.read_excel(path2, sheet_name=None, index_col=0, squeeze=True)

            self.assert_eq(
                ps.read_excel(tmp, sheet_name="Sheet_name_2", index_col=0).sort_index(),
                pd.concat([pdfs1["Sheet_name_2"], pdfs2["Sheet_name_2"]]).sort_index(),
            )
            self.assert_eq(
                ps.read_excel(
                    tmp, sheet_name="Sheet_name_2", index_col=0, squeeze=True
                ).sort_index(),
                pd.concat(
                    [pdfs1_squeezed["Sheet_name_2"], pdfs2_squeezed["Sheet_name_2"]]
                ).sort_index(),
            )

            for sheet_name in sheet_names:
                psdfs = ps.read_excel(tmp, sheet_name=sheet_name, index_col=0)
                self.assert_eq(
                    psdfs["Sheet_name_1"].sort_index(),
                    pd.concat([pdfs1["Sheet_name_1"], pdfs2["Sheet_name_1"]]).sort_index(),
                )
                self.assert_eq(
                    psdfs["Sheet_name_2"].sort_index(),
                    pd.concat([pdfs1["Sheet_name_2"], pdfs2["Sheet_name_2"]]).sort_index(),
                )

                psdfs = ps.read_excel(tmp, sheet_name=sheet_name, index_col=0, squeeze=True)
                self.assert_eq(
                    psdfs["Sheet_name_1"].sort_index(),
                    pd.concat(
                        [pdfs1_squeezed["Sheet_name_1"], pdfs2_squeezed["Sheet_name_1"]]
                    ).sort_index(),
                )
                self.assert_eq(
                    psdfs["Sheet_name_2"].sort_index(),
                    pd.concat(
                        [pdfs1_squeezed["Sheet_name_2"], pdfs2_squeezed["Sheet_name_2"]]
                    ).sort_index(),
                )

    def test_read_orc(self):
        with self.temp_dir() as tmp:
            path = "{}/file1.orc".format(tmp)
            data = self.test_pdf
            self.spark.createDataFrame(data, "i32 int, i64 long, f double, bhello string").coalesce(
                1
            ).write.orc(path, mode="overwrite")

            expected = data.reset_index()[data.columns]
            actual = ps.read_orc(path)
            self.assertPandasEqual(expected, actual.to_pandas())

            # columns
            columns = ["i32", "i64"]
            expected = data.reset_index()[columns]
            actual = ps.read_orc(path, columns=columns)
            self.assertPandasEqual(expected, actual.to_pandas())

            # index_col
            expected = data.set_index("i32")
            actual = ps.read_orc(path, index_col="i32")
            self.assert_eq(actual, expected)

            expected = data.set_index(["i32", "f"])
            actual = ps.read_orc(path, index_col=["i32", "f"])
            self.assert_eq(actual, expected)

            # index_col with columns
            expected = data.set_index("i32")[["i64", "bhello"]]
            actual = ps.read_orc(path, index_col=["i32"], columns=["i64", "bhello"])
            self.assert_eq(actual, expected)

            expected = data.set_index(["i32", "f"])[["bhello", "i64"]]
            actual = ps.read_orc(path, index_col=["i32", "f"], columns=["bhello", "i64"])
            self.assert_eq(actual, expected)

            msg = "Unknown column name 'i'"
            with self.assertRaises(ValueError, msg=msg):
                ps.read_orc(path, columns="i32")
            msg = "Unknown column name 'i34'"
            with self.assertRaises(ValueError, msg=msg):
                ps.read_orc(path, columns=["i34", "i64"])

    def test_orc_write(self):
        with self.temp_dir() as tmp:
            pdf = self.test_pdf
            expected = ps.DataFrame(pdf)

            # Write out partitioned by one column
            expected.to_orc(tmp, mode="overwrite", partition_cols="i32")
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_orc(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Write out partitioned by two columns
            expected.to_orc(tmp, mode="overwrite", partition_cols=["i32", "bhello"])
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_orc(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )

            # Test `options` parameter
            expected.to_orc(
                tmp, mode="overwrite", partition_cols="i32", options={"compression": "none"}
            )
            # Reset column order, as once the data is written out, Spark rearranges partition
            # columns to appear first.
            actual = ps.read_orc(tmp)
            self.assertFalse((actual.columns == self.test_column_order).all())
            actual = actual[self.test_column_order]
            self.assert_eq(
                actual.sort_values(by="f").to_spark().toPandas(),
                expected.sort_values(by="f").to_spark().toPandas(),
            )


if __name__ == "__main__":
    from pyspark.pandas.tests.test_dataframe_spark_io import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
