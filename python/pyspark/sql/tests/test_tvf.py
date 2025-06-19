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

        # empty
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

    def test_explode_with_lateral_join(self):
        with self.tempView("t1", "t2"):
            t1 = self.spark.sql("VALUES (0, 1), (1, 2) AS t1(c1, c2)")
            t1.createOrReplaceTempView("t1")
            t3 = self.spark.sql(
                "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4)) "
                "AS t3(c1, c2)"
            )
            t3.createOrReplaceTempView("t3")

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.explode(sf.array(sf.col("c1").outer(), sf.col("c2").outer()))
                    .toDF("c3")
                    .alias("t2")
                ),
                self.spark.sql("""SELECT * FROM t1, LATERAL EXPLODE(ARRAY(c1, c2)) t2(c3)"""),
            )
            assertDataFrameEqual(
                t3.lateralJoin(self.spark.tvf.explode(sf.col("c2").outer()).toDF("v").alias("t2")),
                self.spark.sql("""SELECT * FROM t3, LATERAL EXPLODE(c2) t2(v)"""),
            )
            assertDataFrameEqual(
                self.spark.tvf.explode(sf.array(sf.lit(1), sf.lit(2)))
                .toDF("v")
                .lateralJoin(
                    self.spark.range(1).select((sf.col("v").outer() + sf.lit(1)).alias("v2"))
                ),
                self.spark.sql(
                    """SELECT * FROM EXPLODE(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1 AS v2)"""
                ),
            )

    def test_explode_outer(self):
        actual = self.spark.tvf.explode_outer(sf.array(sf.lit(1), sf.lit(2)))
        expected = self.spark.sql("""SELECT * FROM explode_outer(array(1, 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.explode_outer(
            sf.create_map(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2))
        )
        expected = self.spark.sql("""SELECT * FROM explode_outer(map('a', 1, 'b', 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # empty
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

    def test_explode_outer_with_lateral_join(self):
        with self.tempView("t1", "t2"):
            t1 = self.spark.sql("VALUES (0, 1), (1, 2) AS t1(c1, c2)")
            t1.createOrReplaceTempView("t1")
            t3 = self.spark.sql(
                "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4)) "
                "AS t3(c1, c2)"
            )
            t3.createOrReplaceTempView("t3")

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.explode_outer(
                        sf.array(sf.col("c1").outer(), sf.col("c2").outer())
                    )
                    .toDF("c3")
                    .alias("t2")
                ),
                self.spark.sql("""SELECT * FROM t1, LATERAL EXPLODE_OUTER(ARRAY(c1, c2)) t2(c3)"""),
            )
            assertDataFrameEqual(
                t3.lateralJoin(
                    self.spark.tvf.explode_outer(sf.col("c2").outer()).toDF("v").alias("t2")
                ),
                self.spark.sql("""SELECT * FROM t3, LATERAL EXPLODE_OUTER(c2) t2(v)"""),
            )
            assertDataFrameEqual(
                self.spark.tvf.explode_outer(sf.array(sf.lit(1), sf.lit(2)))
                .toDF("v")
                .lateralJoin(
                    self.spark.range(1).select((sf.col("v").outer() + sf.lit(1)).alias("v2"))
                ),
                self.spark.sql(
                    """
                    SELECT * FROM EXPLODE_OUTER(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1 AS v2)
                    """
                ),
            )

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

    def test_inline_with_lateral_join(self):
        with self.tempView("array_struct"):
            array_struct = self.spark.sql(
                """
                VALUES
                (1, ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))),
                (2, ARRAY()),
                (3, ARRAY(STRUCT(3, 'c'))) AS array_struct(id, arr)
                """
            )
            array_struct.createOrReplaceTempView("array_struct")

            assertDataFrameEqual(
                array_struct.lateralJoin(self.spark.tvf.inline(sf.col("arr").outer())),
                self.spark.sql("""SELECT * FROM array_struct JOIN LATERAL INLINE(arr)"""),
            )
            assertDataFrameEqual(
                array_struct.lateralJoin(
                    self.spark.tvf.inline(sf.col("arr").outer()).toDF("k", "v").alias("t"),
                    sf.col("id") == sf.col("k"),
                    "left",
                ),
                self.spark.sql(
                    """
                    SELECT * FROM array_struct LEFT JOIN LATERAL INLINE(arr) t(k, v) ON id = k
                    """
                ),
            )

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

    def test_inline_outer_with_lateral_join(self):
        with self.tempView("array_struct"):
            array_struct = self.spark.sql(
                """
                VALUES
                (1, ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))),
                (2, ARRAY()),
                (3, ARRAY(STRUCT(3, 'c'))) AS array_struct(id, arr)
                """
            )
            array_struct.createOrReplaceTempView("array_struct")

            assertDataFrameEqual(
                array_struct.lateralJoin(self.spark.tvf.inline_outer(sf.col("arr").outer())),
                self.spark.sql("""SELECT * FROM array_struct JOIN LATERAL INLINE_OUTER(arr)"""),
            )
            assertDataFrameEqual(
                array_struct.lateralJoin(
                    self.spark.tvf.inline_outer(sf.col("arr").outer()).toDF("k", "v").alias("t"),
                    sf.col("id") == sf.col("k"),
                    "left",
                ),
                self.spark.sql(
                    """
                    SELECT * FROM array_struct LEFT JOIN LATERAL INLINE_OUTER(arr) t(k, v) ON id = k
                    """
                ),
            )

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

    def test_json_tuple_with_lateral_join(self):
        with self.tempView("json_table"):
            json_table = self.spark.sql(
                """
                VALUES
                ('1', '{"f1": "1", "f2": "2", "f3": 3, "f5": 5.23}'),
                ('2', '{"f1": "1", "f3": "3", "f2": 2, "f4": 4.01}'),
                ('3', '{"f1": 3, "f4": "4", "f3": "3", "f2": 2, "f5": 5.01}'),
                ('4', cast(null as string)),
                ('5', '{"f1": null, "f5": ""}'),
                ('6', '[invalid JSON string]') AS json_table(key, jstring)
                """
            )
            json_table.createOrReplaceTempView("json_table")

            assertDataFrameEqual(
                json_table.alias("t1")
                .lateralJoin(
                    self.spark.tvf.json_tuple(
                        sf.col("jstring").outer(),
                        sf.lit("f1"),
                        sf.lit("f2"),
                        sf.lit("f3"),
                        sf.lit("f4"),
                        sf.lit("f5"),
                    ).alias("t2")
                )
                .select("t1.key", "t2.*"),
                self.spark.sql(
                    """
                    SELECT t1.key, t2.* FROM json_table t1,
                    LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2
                    """
                ),
            )
            assertDataFrameEqual(
                json_table.alias("t1")
                .lateralJoin(
                    self.spark.tvf.json_tuple(
                        sf.col("jstring").outer(),
                        sf.lit("f1"),
                        sf.lit("f2"),
                        sf.lit("f3"),
                        sf.lit("f4"),
                        sf.lit("f5"),
                    ).alias("t2")
                )
                .where(sf.col("t2.c0").isNotNull())
                .select("t1.key", "t2.*"),
                self.spark.sql(
                    """
                    SELECT t1.key, t2.* FROM json_table t1,
                    LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2
                    WHERE t2.c0 IS NOT NULL
                    """
                ),
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

        # empty
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

    def test_posexplode_with_lateral_join(self):
        with self.tempView("t1", "t2"):
            t1 = self.spark.sql("VALUES (0, 1), (1, 2) AS t1(c1, c2)")
            t1.createOrReplaceTempView("t1")
            t3 = self.spark.sql(
                "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4)) "
                "AS t3(c1, c2)"
            )
            t3.createOrReplaceTempView("t3")

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.posexplode(sf.array(sf.col("c1").outer(), sf.col("c2").outer()))
                ),
                self.spark.sql("""SELECT * FROM t1, LATERAL POSEXPLODE(ARRAY(c1, c2))"""),
            )
            assertDataFrameEqual(
                t3.lateralJoin(self.spark.tvf.posexplode(sf.col("c2").outer())),
                self.spark.sql("""SELECT * FROM t3, LATERAL POSEXPLODE(c2)"""),
            )
            assertDataFrameEqual(
                self.spark.tvf.posexplode(sf.array(sf.lit(1), sf.lit(2)))
                .toDF("p", "v")
                .lateralJoin(
                    self.spark.range(1).select((sf.col("v").outer() + sf.lit(1)).alias("v2"))
                ),
                self.spark.sql(
                    """
                    SELECT * FROM POSEXPLODE(ARRAY(1, 2)) t(p, v), LATERAL (SELECT v + 1 AS v2)
                    """
                ),
            )

    def test_posexplode_outer(self):
        actual = self.spark.tvf.posexplode_outer(sf.array(sf.lit(1), sf.lit(2)))
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(array(1, 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        actual = self.spark.tvf.posexplode_outer(
            sf.create_map(sf.lit("a"), sf.lit(1), sf.lit("b"), sf.lit(2))
        )
        expected = self.spark.sql("""SELECT * FROM posexplode_outer(map('a', 1, 'b', 2))""")
        assertDataFrameEqual(actual=actual, expected=expected)

        # empty
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

    def test_posexplode_outer_with_lateral_join(self):
        with self.tempView("t1", "t2"):
            t1 = self.spark.sql("VALUES (0, 1), (1, 2) AS t1(c1, c2)")
            t1.createOrReplaceTempView("t1")
            t3 = self.spark.sql(
                "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4)) "
                "AS t3(c1, c2)"
            )
            t3.createOrReplaceTempView("t3")

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.posexplode_outer(
                        sf.array(sf.col("c1").outer(), sf.col("c2").outer())
                    )
                ),
                self.spark.sql("""SELECT * FROM t1, LATERAL POSEXPLODE_OUTER(ARRAY(c1, c2))"""),
            )
            assertDataFrameEqual(
                t3.lateralJoin(self.spark.tvf.posexplode_outer(sf.col("c2").outer())),
                self.spark.sql("""SELECT * FROM t3, LATERAL POSEXPLODE_OUTER(c2)"""),
            )
            assertDataFrameEqual(
                self.spark.tvf.posexplode_outer(sf.array(sf.lit(1), sf.lit(2)))
                .toDF("p", "v")
                .lateralJoin(
                    self.spark.range(1).select((sf.col("v").outer() + sf.lit(1)).alias("v2"))
                ),
                self.spark.sql(
                    """
                    SELECT * FROM POSEXPLODE_OUTER(ARRAY(1, 2)) t(p, v),
                        LATERAL (SELECT v + 1 AS v2)
                    """
                ),
            )

    def test_stack(self):
        actual = self.spark.tvf.stack(sf.lit(2), sf.lit(1), sf.lit(2), sf.lit(3))
        expected = self.spark.sql("""SELECT * FROM stack(2, 1, 2, 3)""")
        assertDataFrameEqual(actual=actual, expected=expected)

    def test_stack_with_lateral_join(self):
        with self.tempView("t1", "t3"):
            t1 = self.spark.sql("VALUES (0, 1), (1, 2) AS t1(c1, c2)")
            t1.createOrReplaceTempView("t1")
            t3 = self.spark.sql(
                "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4)) "
                "AS t3(c1, c2)"
            )
            t3.createOrReplaceTempView("t3")

            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.stack(
                        sf.lit(2),
                        sf.lit("Key"),
                        sf.col("c1").outer(),
                        sf.lit("Value"),
                        sf.col("c2").outer(),
                    ).alias("t")
                ).select("t.*"),
                self.spark.sql(
                    """SELECT t.* FROM t1, LATERAL stack(2, 'Key', c1, 'Value', c2) t"""
                ),
            )
            assertDataFrameEqual(
                t1.lateralJoin(
                    self.spark.tvf.stack(sf.lit(1), sf.col("c1").outer(), sf.col("c2").outer())
                    .toDF("x", "y")
                    .alias("t")
                ).select("t.*"),
                self.spark.sql("""SELECT t.* FROM t1 JOIN LATERAL stack(1, c1, c2) t(x, y)"""),
            )
            assertDataFrameEqual(
                t1.join(t3, sf.col("t1.c1") == sf.col("t3.c1"))
                .lateralJoin(
                    self.spark.tvf.stack(
                        sf.lit(1), sf.col("t1.c2").outer(), sf.col("t3.c2").outer()
                    ).alias("t")
                )
                .select("t.*"),
                self.spark.sql(
                    """
                    SELECT t.* FROM t1 JOIN t3 ON t1.c1 = t3.c1
                        JOIN LATERAL stack(1, t1.c2, t3.c2) t
                    """
                ),
            )

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

        # empty
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

    def test_variant_explode_with_lateral_join(self):
        with self.tempView("variant_table"):
            variant_table = self.spark.sql(
                """
                SELECT id, parse_json(v) AS v FROM VALUES
                    (0, '["hello", "world"]'), (1, '{"a": true, "b": 3.14}'),
                    (2, '[]'), (3, '{}'),
                    (4, NULL), (5, '1')
                    AS t(id, v)
                """
            )
            variant_table.createOrReplaceTempView("variant_table")

            assertDataFrameEqual(
                variant_table.alias("t1")
                .lateralJoin(self.spark.tvf.variant_explode(sf.col("v").outer()).alias("t"))
                .select("t1.id", "t.*"),
                self.spark.sql(
                    """
                    SELECT t1.id, t.* FROM variant_table AS t1,
                        LATERAL variant_explode(v) AS t
                    """
                ),
            )

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

        # empty
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

    def test_variant_explode_outer_with_lateral_join(self):
        with self.tempView("variant_table"):
            variant_table = self.spark.sql(
                """
                SELECT id, parse_json(v) AS v FROM VALUES
                    (0, '["hello", "world"]'), (1, '{"a": true, "b": 3.14}'),
                    (2, '[]'), (3, '{}'),
                    (4, NULL), (5, '1')
                    AS t(id, v)
                """
            )
            variant_table.createOrReplaceTempView("variant_table")

            assertDataFrameEqual(
                variant_table.alias("t1")
                .lateralJoin(self.spark.tvf.variant_explode_outer(sf.col("v").outer()).alias("t"))
                .select("t1.id", "t.*"),
                self.spark.sql(
                    """
                    SELECT t1.id, t.* FROM variant_table AS t1,
                        LATERAL variant_explode_outer(v) AS t
                    """
                ),
            )


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
