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

import os
from contextlib import contextmanager

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


def normalize_text(s):
    return "\n".join(map(str.strip, s.strip().split("\n")))


class CsvTestsMixin:
    @property
    def csv_text(self):
        return normalize_text(
            """
            name,amount
            Alice,100
            Bob,-200
            Charlie,300
            Dennis,400
            Edith,-500
            Frank,600
            Alice,200
            Frank,-200
            Bob,600
            Alice,400
            Frank,200
            Alice,300
            Edith,600
            """
        )

    @property
    def csv_text_2(self):
        return normalize_text(
            """
            A,B
            item1,1
            item2,1,2
            item3,1,2,3,4
            item4,1
            """
        )

    @property
    def csv_text_with_comments(self):
        return normalize_text(
            """
            # header
            %s
            # comment
            Alice,400
            Edith,600
            # footer
            """
            % self.csv_text
        )

    @property
    def tab_delimited_csv_text(self):
        return normalize_text(
            """
            name\tamount
            Alice\t100
            Bob\t-200
            Charlie\t300
            """
        )

    @property
    def q_quoted_csv_text(self):
        return normalize_text(
            """
            QnameQ,QamountQ
            QA,liceQ,Q100Q
            QB,obQ,Q-200Q
            QC,harlieQ,Q300Q
            """
        )

    @property
    def e_escapeted_csv_text(self):
        return normalize_text(
            """
            name,amount
            "AE"lice",100
            "BE"ob",-200
            "CE"harlie",300
            """
        )

    @contextmanager
    def csv_file(self, csv):
        with self.temp_file() as tmp:
            with open(tmp, "w") as f:
                f.write(csv)
            yield tmp

    def test_read_csv(self):
        with self.csv_file(self.csv_text) as fn:

            def check(header="infer", names=None, usecols=None, index_col=None):
                expected = pd.read_csv(
                    fn, header=header, names=names, usecols=usecols, index_col=index_col
                )
                actual = ps.read_csv(
                    fn, header=header, names=names, usecols=usecols, index_col=index_col
                )
                self.assert_eq(expected, actual, almost=True)

            check()
            check(header=0)
            check(header=None)
            check(names=["n", "a"])
            check(names=[("x", "n"), ("y", "a")])
            check(names=[10, 20])
            check(header=0, names=["n", "a"])
            check(usecols=[1])
            check(usecols=[1, 0])
            check(usecols=["amount"])
            check(usecols=["amount", "name"])
            check(usecols=[])
            check(usecols=[1, 1])
            check(usecols=["amount", "amount"])
            check(header=None, usecols=[1])
            check(names=["n", "a"], usecols=["a"])
            check(header=None, names=["n", "a"], usecols=["a"])
            check(index_col=["amount"])
            check(header=None, index_col=[1])
            check(names=["n", "a"], index_col=["a"])
            check(names=["n", "a"], index_col="a")

            # check with pyspark patch.
            expected = pd.read_csv(fn)
            actual = ps.read_csv(fn)
            self.assert_eq(expected, actual, almost=True)

            self.assertRaisesRegex(
                ValueError, "non-unique", lambda: ps.read_csv(fn, names=["n", "n"])
            )
            self.assertRaisesRegex(
                ValueError,
                "does not match the number.*3",
                lambda: ps.read_csv(fn, names=["n", "a", "b"]),
            )
            self.assertRaisesRegex(
                ValueError,
                "does not match the number.*3",
                lambda: ps.read_csv(fn, header=0, names=["n", "a", "b"]),
            )
            self.assertRaisesRegex(
                ValueError, "Usecols do not match.*3", lambda: ps.read_csv(fn, usecols=[1, 3])
            )
            self.assertRaisesRegex(
                ValueError,
                "Usecols do not match.*col",
                lambda: ps.read_csv(fn, usecols=["amount", "col"]),
            )
            self.assertRaisesRegex(
                ValueError, "Unknown header argument 1", lambda: ps.read_csv(fn, header="1")
            )
            expected_error_message = (
                "'usecols' must either be list-like of all strings, "
                "all unicode, all integers or a callable."
            )
            self.assertRaisesRegex(
                ValueError, expected_error_message, lambda: ps.read_csv(fn, usecols=[1, "amount"])
            )

            # check with index_col
            expected = pd.read_csv(fn).set_index("name")
            actual = ps.read_csv(fn, index_col="name")
            self.assert_eq(expected, actual, almost=True)

    def test_read_with_spark_schema(self):
        with self.csv_file(self.csv_text_2) as fn:
            actual = ps.read_csv(fn, names="A string, B string, C long, D long, E long")
            expected = pd.read_csv(fn, names=["A", "B", "C", "D", "E"])
            self.assert_eq(expected, actual)

    def test_read_csv_with_comment(self):
        with self.csv_file(self.csv_text_with_comments) as fn:
            expected = pd.read_csv(fn, comment="#")
            actual = ps.read_csv(fn, comment="#")
            self.assert_eq(expected, actual, almost=True)

            self.assertRaisesRegex(
                ValueError,
                "Only length-1 comment characters supported",
                lambda: ps.read_csv(fn, comment="").show(),
            )
            self.assertRaisesRegex(
                ValueError,
                "Only length-1 comment characters supported",
                lambda: ps.read_csv(fn, comment="##").show(),
            )
            self.assertRaisesRegex(
                ValueError,
                "Only length-1 comment characters supported",
                lambda: ps.read_csv(fn, comment=1),
            )
            self.assertRaisesRegex(
                ValueError,
                "Only length-1 comment characters supported",
                lambda: ps.read_csv(fn, comment=[1]),
            )

    def test_read_csv_with_limit(self):
        with self.csv_file(self.csv_text_with_comments) as fn:
            expected = pd.read_csv(fn, comment="#", nrows=2)
            actual = ps.read_csv(fn, comment="#", nrows=2)
            self.assert_eq(expected, actual, almost=True)

    def test_read_csv_with_encoding(self):
        # SPARK-37181: Read csv supporting latin-1 encoding.
        with self.csv_file(self.csv_text) as fn:
            expected = pd.read_csv(fn, encoding="latin-1")
            actual = ps.read_csv(fn, encoding="latin-1")
            self.assert_eq(expected, actual, almost=True)

    def test_read_csv_with_sep(self):
        with self.csv_file(self.tab_delimited_csv_text) as fn:
            expected = pd.read_csv(fn, sep="\t")
            actual = ps.read_csv(fn, sep="\t")
            self.assert_eq(expected, actual, almost=True)

    def test_read_csv_with_parse_dates(self):
        self.assertRaisesRegex(
            ValueError, "parse_dates", lambda: ps.read_csv("path", parse_dates=True)
        )

    def test_read_csv_with_dtype(self):
        with self.csv_file(self.csv_text) as fn:
            self.assert_eq(ps.read_csv(fn), pd.read_csv(fn), almost=True)
            self.assert_eq(ps.read_csv(fn, dtype=str), pd.read_csv(fn, dtype=str))
            self.assert_eq(
                ps.read_csv(fn, dtype={"amount": "int64"}),
                pd.read_csv(fn, dtype={"amount": "int64"}),
            )

    def test_read_csv_with_quotechar(self):
        with self.csv_file(self.q_quoted_csv_text) as fn:
            self.assert_eq(
                ps.read_csv(fn, quotechar="Q"), pd.read_csv(fn, quotechar="Q"), almost=True
            )

    def test_read_csv_with_escapechar(self):
        with self.csv_file(self.e_escapeted_csv_text) as fn:
            self.assert_eq(
                ps.read_csv(fn, escapechar="E"), pd.read_csv(fn, escapechar="E"), almost=True
            )

            self.assert_eq(
                ps.read_csv(fn, escapechar="ABC", escape="E"),
                pd.read_csv(fn, escapechar="E"),
                almost=True,
            )

    def test_to_csv(self):
        pdf = pd.DataFrame({"aa": [1, 2, 3], "bb": [4, 5, 6]}, index=[0, 1, 3])
        psdf = ps.DataFrame(pdf)

        self.assert_eq(psdf.to_csv(), pdf.to_csv(index=False))
        self.assert_eq(psdf.to_csv(columns=["aa"]), pdf.to_csv(columns=["aa"], index=False))
        self.assert_eq(psdf.aa.to_csv(), pdf.aa.to_csv(index=False, header=True))

        pdf = pd.DataFrame({"a": [1, np.nan, 3], "b": ["one", "two", None]}, index=[0, 1, 3])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.to_csv(na_rep="null"), pdf.to_csv(na_rep="null", index=False))
        self.assert_eq(
            psdf.a.to_csv(na_rep="null"), pdf.a.to_csv(na_rep="null", index=False, header=True)
        )

        self.assertRaises(KeyError, lambda: psdf.to_csv(columns=["ab"]))

        pdf = pd.DataFrame({"a": [1.0, 2.0, 3.0], "b": [4.0, 5.0, 6.0]}, index=[0, 1, 3])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.to_csv(), pdf.to_csv(index=False))
        self.assert_eq(psdf.to_csv(header=False), pdf.to_csv(header=False, index=False))
        self.assert_eq(psdf.to_csv(), pdf.to_csv(index=False))

        # non-string names
        pdf = pd.DataFrame({10: [1, 2, 3], 20: [4, 5, 6]}, index=[0, 1, 3])
        psdf = ps.DataFrame(pdf)

        self.assert_eq(psdf.to_csv(), pdf.to_csv(index=False))
        self.assert_eq(psdf.to_csv(columns=[10]), pdf.to_csv(columns=[10], index=False))

        self.assertRaises(TypeError, lambda: psdf.to_csv(columns=10))

    def _check_output(self, dir, expected):
        output_paths = [path for path in os.listdir(dir) if path.startswith("part-")]
        assert len(output_paths) > 0
        output_path = "%s/%s" % (dir, output_paths[0])
        with open(output_path) as f:
            self.assertEqual(f.read(), expected)

    def test_to_csv_with_path(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        psdf = ps.DataFrame(pdf)

        with self.temp_dir() as dirpath:
            tmp_dir = "{}/tmp1".format(dirpath)

            psdf.to_csv(tmp_dir, num_files=1)
            self._check_output(tmp_dir, pdf.to_csv(index=False))

            tmp_dir = "{}/tmp2".format(dirpath)

            self.assertRaises(KeyError, lambda: psdf.to_csv(tmp_dir, columns=["c"], num_files=1))

            # non-string names
            pdf = pd.DataFrame({10: [1, 2, 3], 20: ["a", "b", "c"]})
            psdf = ps.DataFrame(pdf)

            tmp_dir = "{}/tmp3".format(dirpath)

            psdf.to_csv(tmp_dir, num_files=1)
            self._check_output(tmp_dir, pdf.to_csv(index=False))

            tmp_dir = "{}/tmp4".format(dirpath)

            psdf.to_csv(tmp_dir, columns=[10], num_files=1)
            self._check_output(tmp_dir, pdf.to_csv(columns=[10], index=False))

            tmp_dir = "{}/tmp5".format(dirpath)

            self.assertRaises(TypeError, lambda: psdf.to_csv(tmp_dir, columns=10, num_files=1))

    def test_to_csv_with_path_and_basic_options(self):
        pdf = pd.DataFrame({"aa": [1, 2, 3], "bb": ["a", "b", "c"]})
        psdf = ps.DataFrame(pdf)

        with self.temp_dir() as dirpath:
            psdf.to_csv(dirpath, num_files=1, sep="|", header=False, columns=["aa"])
            expected = pdf.to_csv(index=False, sep="|", header=False, columns=["aa"])

            self._check_output(dirpath, expected)

    def test_to_csv_with_path_and_basic_options_multiindex_columns(self):
        pdf = pd.DataFrame({("x", "a"): [1, 2, 3], ("y", "b"): ["a", "b", "c"]})
        psdf = ps.DataFrame(pdf)

        with self.temp_dir() as dirpath:
            with self.assertRaises(ValueError):
                psdf.to_csv(dirpath, num_files=1, sep="|", columns=[("x", "a")])

            psdf.to_csv(dirpath, num_files=1, sep="|", header=["a"], columns=[("x", "a")])
            pdf.columns = ["a", "b"]
            expected = pdf.to_csv(index=False, sep="|", columns=["a"])

            self._check_output(dirpath, expected)

    def test_to_csv_with_path_and_pyspark_options(self):
        pdf = pd.DataFrame({"a": [1, 2, 3, None], "b": ["a", "b", "c", None]})
        psdf = ps.DataFrame(pdf)

        with self.temp_dir() as dirpath:
            psdf.to_csv(dirpath, nullValue="null", num_files=1)
            expected = pdf.to_csv(index=False, na_rep="null")

            self._check_output(dirpath, expected)

    def test_to_csv_with_partition_cols(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        psdf = ps.DataFrame(pdf)

        with self.temp_dir() as dirpath:
            psdf.to_csv(dirpath, partition_cols="b", num_files=1)

            partition_paths = [path for path in os.listdir(dirpath) if path.startswith("b=")]
            assert len(partition_paths) > 0
            for partition_path in partition_paths:
                column, value = partition_path.split("=")
                expected = pdf[pdf[column] == value].drop("b", axis=1).to_csv(index=False)

                output_paths = [
                    path
                    for path in os.listdir("%s/%s" % (dirpath, partition_path))
                    if path.startswith("part-")
                ]
                assert len(output_paths) > 0
                output_path = "%s/%s/%s" % (dirpath, partition_path, output_paths[0])
                with open(output_path) as f:
                    self.assertEqual(f.read(), expected)


class CsvTests(
    CsvTestsMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.io.test_csv import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
