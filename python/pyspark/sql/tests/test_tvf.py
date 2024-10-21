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

from pyspark.errors import PySparkValueError
from pyspark.sql import functions as sf
from pyspark.testing import assertDataFrameEqual
from pyspark.testing.sqlutils import ReusedSQLTestCase


class TVFTestsMixin:
    def test_explode(self):
        actual = self.spark.tvf.explode(sf.array(sf.lit(1), sf.lit(2)))
        expected = self.spark.sql("""SELECT * FROM explode(array(1, 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode(
            sf.create_map(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2))
        )
        expected = self.spark.sql("""SELECT * FROM explode(map('a', 1, 'b', 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # emtpy
        actual = self.spark.tvf.explode(sf.array())
        expected = self.spark.sql("""SELECT * FROM explode(array())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode(sf.create_map())
        expected = self.spark.sql("""SELECT * FROM explode(map())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # null
        actual = self.spark.tvf.explode(sf.lit(None).astype("array<int>"))
        expected = self.spark.sql("""SELECT * FROM explode(null :: array<int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode(sf.lit(None).astype("map<string, int>"))
        expected = self.spark.sql("""SELECT * FROM explode(null :: map<string, int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_explode_outer(self):
        actual = self.spark.tvf.explode_outer(sf.array(sf.lit(1), sf.lit(2)))
        expected = self.spark.sql("""SELECT * FROM explode_outer(array(1, 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode_outer(
            sf.create_map(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2))
        )
        expected = self.spark.sql("""SELECT * FROM explode_outer(map('a', 1, 'b', 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # emtpy
        actual = self.spark.tvf.explode_outer(sf.array())
        expected = self.spark.sql("""SELECT * FROM explode_outer(array())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode_outer(sf.create_map())
        expected = self.spark.sql("""SELECT * FROM explode_outer(map())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # null
        actual = self.spark.tvf.explode_outer(sf.lit(None).astype("array<int>"))
        expected = self.spark.sql("""SELECT * FROM explode_outer(null :: array<int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode_outer(sf.lit(None).astype("map<string, int>"))
        expected = self.spark.sql("""SELECT * FROM explode_outer(null :: map<string, int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_inline(self):
        actual = self.spark.tvf.inline(
            sf.array(sf.struct(sf.lit(1), sf.lit("a")), sf.struct(sf.lit(2), sf.lit("b")))
        )
        expected = self.spark.sql("""SELECT * FROM inline(array(struct(1, 'a'), struct(2, 'b')))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.inline(sf.array().astype("array<struct<a:int,b:int>>"))
        expected = self.spark.sql("""SELECT * FROM inline(array() :: array<struct<a:int,b:int>>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.inline(
            sf.array(
                sf.named_struct(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2)),
                sf.lit(None),
                sf.named_struct(sf.lit("a"), sf.lit(3), sf.lit("b"), sf.lit(4)),
            )
        )
        expected = self.spark.sql(
            """
            SELECT * FROM
              inline(array(named_struct('a', 1, 'b', 2), null, named_struct('a', 3, 'b', 4)))
            """
        )
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_inline_outer(self):
        actual = self.spark.tvf.inline_outer(
            sf.array(sf.struct(sf.lit(1), sf.lit("a")), sf.struct(sf.lit(2), sf.lit("b")))
        )
        expected = self.spark.sql(
            """SELECT * FROM inline_outer(array(struct(1, 'a'), struct(2, 'b')))"""
        )
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.inline_outer(sf.array().astype("array<struct<a:int,b:int>>"))
        expected = self.spark.sql(
            """SELECT * FROM inline_outer(array() :: array<struct<a:int,b:int>>)"""
        )
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.inline_outer(
            sf.array(
                sf.named_struct(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2)),
                sf.lit(None),
                sf.named_struct(sf.lit("a"), sf.lit(3), sf.lit("b"), sf.lit(4)),
            )
        )
        expected = self.spark.sql(
            """
            SELECT * FROM
              inline_outer(array(named_struct('a', 1, 'b', 2), null, named_struct('a', 3, 'b', 4)))
            """
        )
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_json_tuple(self):
        actual = self.spark.tvf.json_tuple(sf.lit('{"a":1, "b":2}'), sf.lit("a"), sf.lit("b"))
        expected = self.spark.sql("""SELECT json_tuple('{"a":1, "b":2}', 'a', 'b')""")
        assertDataFrameEqual(actual=actual, expected=expected)

        with self.assertRaises(PySparkValueError) as pe:
            self.spark.tvf.json_tuple(sf.lit('{"a":1, "b":2}'))

        self.check_error(
            exception=pe.exception,
            errorClass="CANNOT_BE_EMPTY",
            messageParameters={"item": "field"},
        )

    def test_posexplode(self):
        actual = self.spark.tvf.posexplode(sf.array(sf.lit(1), sf.lit(2)))
        expected = self.spark.sql("""SELECT * FROM posexplode(array(1, 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode(
            sf.create_map(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2))
        )
        expected = self.spark.sql("""SELECT * FROM posexplode(map('a', 1, 'b', 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # emtpy
        actual = self.spark.tvf.posexplode(sf.array())
        expected = self.spark.sql("""SELECT * FROM posexplode(array())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode(sf.create_map())
        expected = self.spark.sql("""SELECT * FROM posexplode(map())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # null
        actual = self.spark.tvf.posexplode(sf.lit(None).astype("array<int>"))
        expected = self.spark.sql("""SELECT * FROM posexplode(null :: array<int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode(sf.lit(None).astype("map<string, int>"))
        expected = self.spark.sql("""SELECT * FROM posexplode(null :: map<string, int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_posexplode_outer(self):
        actual = self.spark.tvf.posexplode_outer(sf.array(sf.lit(1), sf.lit(2)))
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(array(1, 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode_outer(
            sf.create_map(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2))
        )
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(map('a', 1, 'b', 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # emtpy
        actual = self.spark.tvf.posexplode_outer(sf.array())
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(array())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode_outer(sf.create_map())
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(map())""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # null
        actual = self.spark.tvf.posexplode_outer(sf.lit(None).astype("array<int>"))
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(null :: array<int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode_outer(sf.lit(None).astype("map<string, int>"))
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(null :: map<string, int>)""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_stack(self):
        actual = self.spark.tvf.stack(sf.lit(2), sf.lit(1), sf.lit(2), sf.lit(3))
        expected = self.spark.sql("""SELECT * FROM stack(2, 1, 2, 3)""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_collations(self):
        actual = self.spark.tvf.collations()
        expected = self.spark.sql("""SELECT * FROM collations()""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_sql_keywords(self):
        actual = self.spark.tvf.sql_keywords()
        expected = self.spark.sql("""SELECT * FROM sql_keywords()""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_variant_explode(self):
        actual = self.spark.tvf.variant_explode(sf.parse_json(sf.lit('["hello", "world"]')))
        expected = self.spark.sql(
            """SELECT * FROM variant_explode(parse_json('["hello", "world"]'))"""
        )
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.variant_explode(sf.parse_json(sf.lit('{"a": true, "b": 3.14}')))
        expected = self.spark.sql(
            """SELECT * FROM variant_explode(parse_json('{"a": true, "b": 3.14}'))"""
        )
        assertDataFrameEqual(actual=actual, expected=expected)

        # emtpy
        actual = self.spark.tvf.variant_explode(sf.parse_json(sf.lit("[]")))
        expected = self.spark.sql("""SELECT * FROM variant_explode(parse_json('[]'))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.variant_explode(sf.parse_json(sf.lit("{}")))
        expected = self.spark.sql("""SELECT * FROM variant_explode(parse_json('{}'))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # null
        actual = self.spark.tvf.variant_explode(sf.lit(None).astype("variant"))
        expected = self.spark.sql("""SELECT * FROM variant_explode(null :: variant)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # not a variant object/array
        actual = self.spark.tvf.variant_explode(sf.parse_json(sf.lit("1")))
        expected = self.spark.sql("""SELECT * FROM variant_explode(parse_json('1'))""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_variant_explode_outer(self):
        actual = self.spark.tvf.variant_explode_outer(sf.parse_json(sf.lit('["hello", "world"]')))
        expected = self.spark.sql(
            """SELECT * FROM variant_explode_outer(parse_json('["hello", "world"]'))"""
        )
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.variant_explode_outer(
            sf.parse_json(sf.lit('{"a": true, "b": 3.14}'))
        )
        expected = self.spark.sql(
            """SELECT * FROM variant_explode_outer(parse_json('{"a": true, "b": 3.14}'))"""
        )
        assertDataFrameEqual(actual=actual, expected=expected)

        # emtpy
        actual = self.spark.tvf.variant_explode_outer(sf.parse_json(sf.lit("[]")))
        expected = self.spark.sql("""SELECT * FROM variant_explode_outer(parse_json('[]'))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.variant_explode_outer(sf.parse_json(sf.lit("{}")))
        expected = self.spark.sql("""SELECT * FROM variant_explode_outer(parse_json('{}'))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # null
        actual = self.spark.tvf.variant_explode_outer(sf.lit(None).astype("variant"))
        expected = self.spark.sql("""SELECT * FROM variant_explode_outer(null :: variant)""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # not a variant object/array
        actual = self.spark.tvf.variant_explode_outer(sf.parse_json(sf.lit("1")))
        expected = self.spark.sql("""SELECT * FROM variant_explode_outer(parse_json('1'))""")
        assertDataFrameEqual(actual=actual, expected=expected)


class TVFTests(TVFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_tvf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
