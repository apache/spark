/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameTableValuedFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  test("explode") {
    val actual1 = spark.tvf.explode(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM explode(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.explode(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM explode(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.explode(array())
    val expected3 = spark.sql("SELECT * FROM explode(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.explode(map())
    val expected4 = spark.sql("SELECT * FROM explode(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.explode(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM explode(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.explode(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM explode(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("explode - lateral join") {
    withView("t1", "t3") {
      sql("CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2)")
      sql("CREATE VIEW t3(c1, c2) AS " +
        "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4))")
      val t1 = spark.table("t1")
      val t3 = spark.table("t3")

      checkAnswer(
        t1.lateralJoin(spark.tvf.explode(array($"c1".outer(), $"c2".outer())).toDF("c3").as("t2")),
        sql("SELECT * FROM t1, LATERAL EXPLODE(ARRAY(c1, c2)) t2(c3)")
      )
      checkAnswer(
        t3.lateralJoin(spark.tvf.explode($"c2".outer()).toDF("v").as("t2")),
        sql("SELECT * FROM t3, LATERAL EXPLODE(c2) t2(v)")
      )
      checkAnswer(
        spark.tvf.explode(array(lit(1), lit(2))).toDF("v")
          .lateralJoin(spark.range(1).select($"v".outer() + lit(1))),
        sql("SELECT * FROM EXPLODE(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1)")
      )
    }
  }

  test("explode_outer") {
    val actual1 = spark.tvf.explode_outer(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM explode_outer(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.explode_outer(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM explode_outer(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.explode_outer(array())
    val expected3 = spark.sql("SELECT * FROM explode_outer(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.explode_outer(map())
    val expected4 = spark.sql("SELECT * FROM explode_outer(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.explode_outer(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM explode_outer(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.explode_outer(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM explode_outer(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("explode_outer - lateral join") {
    withView("t1", "t3") {
      sql("CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2)")
      sql("CREATE VIEW t3(c1, c2) AS " +
        "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4))")
      val t1 = spark.table("t1")
      val t3 = spark.table("t3")

      checkAnswer(
        t1.lateralJoin(
          spark.tvf.explode_outer(array($"c1".outer(), $"c2".outer())).toDF("c3").as("t2")),
        sql("SELECT * FROM t1, LATERAL EXPLODE_OUTER(ARRAY(c1, c2)) t2(c3)")
      )
      checkAnswer(
        t3.lateralJoin(spark.tvf.explode_outer($"c2".outer()).toDF("v").as("t2")),
        sql("SELECT * FROM t3, LATERAL EXPLODE_OUTER(c2) t2(v)")
      )
      checkAnswer(
        spark.tvf.explode_outer(array(lit(1), lit(2))).toDF("v")
          .lateralJoin(spark.range(1).select($"v".outer() + lit(1))),
        sql("SELECT * FROM EXPLODE_OUTER(ARRAY(1, 2)) t(v), LATERAL (SELECT v + 1)")
      )
    }
  }

  test("inline") {
    val actual1 = spark.tvf.inline(array(struct(lit(1), lit("a")), struct(lit(2), lit("b"))))
    val expected1 = spark.sql("SELECT * FROM inline(array(struct(1, 'a'), struct(2, 'b')))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.inline(array().cast("array<struct<a:int,b:int>>"))
    val expected2 = spark.sql("SELECT * FROM inline(array() :: array<struct<a:int,b:int>>)")
    checkAnswer(actual2, expected2)

    val actual3 = spark.tvf.inline(array(
      named_struct(lit("a"), lit(1), lit("b"), lit(2)),
      lit(null),
      named_struct(lit("a"), lit(3), lit("b"), lit(4))
    ))
    val expected3 = spark.sql(
      "SELECT * FROM " +
        "inline(array(named_struct('a', 1, 'b', 2), null, named_struct('a', 3, 'b', 4)))")
    checkAnswer(actual3, expected3)
  }

  test("inline - lateral join") {
    withView("array_struct") {
      sql(
        """
          |CREATE VIEW array_struct(id, arr) AS VALUES
          |    (1, ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))),
          |    (2, ARRAY()),
          |    (3, ARRAY(STRUCT(3, 'c')))
          |""".stripMargin)
      val arrayStruct = spark.table("array_struct")

      checkAnswer(
        arrayStruct.lateralJoin(spark.tvf.inline($"arr".outer())),
        sql("SELECT * FROM array_struct JOIN LATERAL INLINE(arr)")
      )
      checkAnswer(
        arrayStruct.lateralJoin(
          spark.tvf.inline($"arr".outer()).toDF("k", "v").as("t"),
          $"id" === $"k",
          "left"
        ),
        sql("SELECT * FROM array_struct LEFT JOIN LATERAL INLINE(arr) t(k, v) ON id = k")
      )
    }
  }

  test("inline_outer") {
    val actual1 = spark.tvf.inline_outer(array(struct(lit(1), lit("a")), struct(lit(2), lit("b"))))
    val expected1 = spark.sql("SELECT * FROM inline_outer(array(struct(1, 'a'), struct(2, 'b')))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.inline_outer(array().cast("array<struct<a:int,b:int>>"))
    val expected2 = spark.sql("SELECT * FROM inline_outer(array() :: array<struct<a:int,b:int>>)")
    checkAnswer(actual2, expected2)

    val actual3 = spark.tvf.inline_outer(array(
      named_struct(lit("a"), lit(1), lit("b"), lit(2)),
      lit(null),
      named_struct(lit("a"), lit(3), lit("b"), lit(4))
    ))
    val expected3 = spark.sql(
      "SELECT * FROM " +
        "inline_outer(array(named_struct('a', 1, 'b', 2), null, named_struct('a', 3, 'b', 4)))")
    checkAnswer(actual3, expected3)
  }

  test("inline_outer - lateral join") {
    withView("array_struct") {
      sql(
        """
          |CREATE VIEW array_struct(id, arr) AS VALUES
          |    (1, ARRAY(STRUCT(1, 'a'), STRUCT(2, 'b'))),
          |    (2, ARRAY()),
          |    (3, ARRAY(STRUCT(3, 'c')))
          |""".stripMargin)
      val arrayStruct = spark.table("array_struct")

      checkAnswer(
        arrayStruct.lateralJoin(spark.tvf.inline_outer($"arr".outer())),
        sql("SELECT * FROM array_struct JOIN LATERAL INLINE_OUTER(arr)")
      )
      checkAnswer(
        arrayStruct.lateralJoin(
          spark.tvf.inline_outer($"arr".outer()).toDF("k", "v").as("t"),
          $"id" === $"k",
          "left"
        ),
        sql("SELECT * FROM array_struct LEFT JOIN LATERAL INLINE_OUTER(arr) t(k, v) ON id = k")
      )
    }
  }

  test("json_tuple") {
    val actual = spark.tvf.json_tuple(lit("""{"a":1,"b":2}"""), lit("a"), lit("b"))
    val expected = spark.sql("""SELECT * FROM json_tuple('{"a":1,"b":2}', 'a', 'b')""")
    checkAnswer(actual, expected)

    val ex = intercept[AnalysisException] {
      spark.tvf.json_tuple(lit("""{"a":1,"b":2}""")).collect()
    }
    assert(ex.errorClass.get == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION")
    assert(ex.messageParameters("functionName") == "`json_tuple`")
  }

  test("json_tuple - lateral join") {
    withView("json_table") {
      sql(
        """
          |CREATE OR REPLACE TEMP VIEW json_table(key, jstring) AS VALUES
          |    ('1', '{"f1": "1", "f2": "2", "f3": 3, "f5": 5.23}'),
          |    ('2', '{"f1": "1", "f3": "3", "f2": 2, "f4": 4.01}'),
          |    ('3', '{"f1": 3, "f4": "4", "f3": "3", "f2": 2, "f5": 5.01}'),
          |    ('4', cast(null as string)),
          |    ('5', '{"f1": null, "f5": ""}'),
          |    ('6', '[invalid JSON string]')
          |""".stripMargin)
      val jsonTable = spark.table("json_table")

      checkAnswer(
        jsonTable.as("t1").lateralJoin(
          spark.tvf.json_tuple(
            $"t1.jstring".outer(),
            lit("f1"), lit("f2"), lit("f3"), lit("f4"), lit("f5")).as("t2")
        ).select($"t1.key", $"t2.*"),
        sql("SELECT t1.key, t2.* FROM json_table t1, " +
          "LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2")
      )
      checkAnswer(
        jsonTable.as("t1").lateralJoin(
          spark.tvf.json_tuple(
            $"jstring".outer(),
            lit("f1"), lit("f2"), lit("f3"), lit("f4"), lit("f5")).as("t2")
        ).where($"t2.c0".isNotNull)
          .select($"t1.key", $"t2.*"),
        sql("SELECT t1.key, t2.* FROM json_table t1, " +
          "LATERAL json_tuple(t1.jstring, 'f1', 'f2', 'f3', 'f4', 'f5') t2 " +
          "WHERE t2.c0 IS NOT NULL")
      )
    }
  }

  test("posexplode") {
    val actual1 = spark.tvf.posexplode(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM posexplode(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.posexplode(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM posexplode(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.posexplode(array())
    val expected3 = spark.sql("SELECT * FROM posexplode(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.posexplode(map())
    val expected4 = spark.sql("SELECT * FROM posexplode(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.posexplode(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM posexplode(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.posexplode(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM posexplode(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("posexplode - lateral join") {
    withView("t1", "t3") {
      sql("CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2)")
      sql("CREATE VIEW t3(c1, c2) AS " +
        "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4))")
      val t1 = spark.table("t1")
      val t3 = spark.table("t3")

      checkAnswer(
        t1.lateralJoin(spark.tvf.posexplode(array($"c1".outer(), $"c2".outer()))),
        sql("SELECT * FROM t1, LATERAL POSEXPLODE(ARRAY(c1, c2))")
      )
      checkAnswer(
        t3.lateralJoin(spark.tvf.posexplode($"c2".outer())),
        sql("SELECT * FROM t3, LATERAL POSEXPLODE(c2)")
      )
      checkAnswer(
        spark.tvf.posexplode(array(lit(1), lit(2))).toDF("p", "v")
          .lateralJoin(spark.range(1).select($"v".outer() + lit(1))),
        sql("SELECT * FROM POSEXPLODE(ARRAY(1, 2)) t(p, v), LATERAL (SELECT v + 1)")
      )
    }
  }

  test("posexplode_outer") {
    val actual1 = spark.tvf.posexplode_outer(array(lit(1), lit(2)))
    val expected1 = spark.sql("SELECT * FROM posexplode_outer(array(1, 2))")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.posexplode_outer(map(lit("a"), lit(1), lit("b"), lit(2)))
    val expected2 = spark.sql("SELECT * FROM posexplode_outer(map('a', 1, 'b', 2))")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.posexplode_outer(array())
    val expected3 = spark.sql("SELECT * FROM posexplode_outer(array())")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.posexplode_outer(map())
    val expected4 = spark.sql("SELECT * FROM posexplode_outer(map())")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.posexplode_outer(lit(null).cast("array<int>"))
    val expected5 = spark.sql("SELECT * FROM posexplode_outer(null :: array<int>)")
    checkAnswer(actual5, expected5)

    val actual6 = spark.tvf.posexplode_outer(lit(null).cast("map<string, int>"))
    val expected6 = spark.sql("SELECT * FROM posexplode_outer(null :: map<string, int>)")
    checkAnswer(actual6, expected6)
  }

  test("posexplode_outer - lateral join") {
    withView("t1", "t3") {
      sql("CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2)")
      sql("CREATE VIEW t3(c1, c2) AS " +
        "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4))")
      val t1 = spark.table("t1")
      val t3 = spark.table("t3")

      checkAnswer(
        t1.lateralJoin(spark.tvf.posexplode_outer(array($"c1".outer(), $"c2".outer()))),
        sql("SELECT * FROM t1, LATERAL POSEXPLODE_OUTER(ARRAY(c1, c2))")
      )
      checkAnswer(
        t3.lateralJoin(spark.tvf.posexplode_outer($"c2".outer())),
        sql("SELECT * FROM t3, LATERAL POSEXPLODE_OUTER(c2)")
      )
      checkAnswer(
        spark.tvf.posexplode_outer(array(lit(1), lit(2))).toDF("p", "v")
          .lateralJoin(spark.range(1).select($"v".outer() + lit(1))),
        sql("SELECT * FROM POSEXPLODE_OUTER(ARRAY(1, 2)) t(p, v), LATERAL (SELECT v + 1)")
      )
    }
  }

  test("stack") {
    val actual = spark.tvf.stack(lit(2), lit(1), lit(2), lit(3))
    val expected = spark.sql("SELECT * FROM stack(2, 1, 2, 3)")
    checkAnswer(actual, expected)
  }

  test("stack - lateral join") {
    withView("t1", "t3") {
      sql("CREATE VIEW t1(c1, c2) AS VALUES (0, 1), (1, 2)")
      sql("CREATE VIEW t3(c1, c2) AS " +
        "VALUES (0, ARRAY(0, 1)), (1, ARRAY(2)), (2, ARRAY()), (null, ARRAY(4))")
      val t1 = spark.table("t1")
      val t3 = spark.table("t3")

      checkAnswer(
        t1.lateralJoin(
          spark.tvf.stack(lit(2), lit("Key"), $"c1".outer(), lit("Value"), $"c2".outer()).as("t")
        ).select($"t.*"),
        sql("SELECT t.* FROM t1, LATERAL stack(2, 'Key', c1, 'Value', c2) t")
      )
      checkAnswer(
        t1.lateralJoin(
          spark.tvf.stack(lit(1), $"c1".outer(), $"c2".outer()).toDF("x", "y").as("t")
        ).select($"t.*"),
        sql("SELECT t.* FROM t1 JOIN LATERAL stack(1, c1, c2) t(x, y)")
      )
      checkAnswer(
        t1.join(t3, $"t1.c1" === $"t3.c1")
          .lateralJoin(
            spark.tvf.stack(lit(1), $"t1.c2".outer(), $"t3.c2".outer()).as("t")
          ).select($"t.*"),
        sql("SELECT t.* FROM t1 JOIN t3 ON t1.c1 = t3.c1 JOIN LATERAL stack(1, t1.c2, t3.c2) t")
      )
    }
  }

  test("collations") {
    val actual = spark.tvf.collations()
    val expected = spark.sql("SELECT * FROM collations()")
    checkAnswer(actual, expected)
  }

  test("sql_keywords") {
    val actual = spark.tvf.sql_keywords()
    val expected = spark.sql("SELECT * FROM sql_keywords()")
    checkAnswer(actual, expected)
  }

  test("variant_explode") {
    val actual1 = spark.tvf.variant_explode(parse_json(lit("""["hello", "world"]""")))
    val expected1 = spark.sql(
      """SELECT * FROM variant_explode(parse_json('["hello", "world"]'))""")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.variant_explode(parse_json(lit("""{"a": true, "b": 3.14}""")))
    val expected2 = spark.sql(
      """SELECT * FROM variant_explode(parse_json('{"a": true, "b": 3.14}'))""")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.variant_explode(parse_json(lit("[]")))
    val expected3 = spark.sql("SELECT * FROM variant_explode(parse_json('[]'))")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.variant_explode(parse_json(lit("{}")))
    val expected4 = spark.sql("SELECT * FROM variant_explode(parse_json('{}'))")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.variant_explode(lit(null).cast("variant"))
    val expected5 = spark.sql("SELECT * FROM variant_explode(null :: variant)")
    checkAnswer(actual5, expected5)

    // not a variant object/array
    val actual6 = spark.tvf.variant_explode(parse_json(lit("1")))
    val expected6 = spark.sql("SELECT * FROM variant_explode(parse_json('1'))")
    checkAnswer(actual6, expected6)
  }

  test("variant_explode - lateral join") {
    withView("variant_table") {
      sql(
        """
          |CREATE VIEW variant_table(id, v) AS
          |SELECT id, parse_json(v) AS v FROM VALUES
          |(0, '["hello", "world"]'), (1, '{"a": true, "b": 3.14}'),
          |(2, '[]'), (3, '{}'),
          |(4, NULL), (5, '1')
          |AS t(id, v)
          |""".stripMargin)
      val variantTable = spark.table("variant_table")

      checkAnswer(
        variantTable.as("t1").lateralJoin(
          spark.tvf.variant_explode($"v".outer()).as("t")
        ).select($"t1.id", $"t.*"),
        sql("SELECT t1.id, t.* FROM variant_table AS t1, LATERAL variant_explode(v) AS t")
      )
    }
  }

  test("variant_explode_outer") {
    val actual1 = spark.tvf.variant_explode_outer(parse_json(lit("""["hello", "world"]""")))
    val expected1 = spark.sql(
      """SELECT * FROM variant_explode_outer(parse_json('["hello", "world"]'))""")
    checkAnswer(actual1, expected1)

    val actual2 = spark.tvf.variant_explode_outer(parse_json(lit("""{"a": true, "b": 3.14}""")))
    val expected2 = spark.sql(
      """SELECT * FROM variant_explode_outer(parse_json('{"a": true, "b": 3.14}'))""")
    checkAnswer(actual2, expected2)

    // empty
    val actual3 = spark.tvf.variant_explode_outer(parse_json(lit("[]")))
    val expected3 = spark.sql("SELECT * FROM variant_explode_outer(parse_json('[]'))")
    checkAnswer(actual3, expected3)

    val actual4 = spark.tvf.variant_explode_outer(parse_json(lit("{}")))
    val expected4 = spark.sql("SELECT * FROM variant_explode_outer(parse_json('{}'))")
    checkAnswer(actual4, expected4)

    // null
    val actual5 = spark.tvf.variant_explode_outer(lit(null).cast("variant"))
    val expected5 = spark.sql("SELECT * FROM variant_explode_outer(null :: variant)")
    checkAnswer(actual5, expected5)

    // not a variant object/array
    val actual6 = spark.tvf.variant_explode_outer(parse_json(lit("1")))
    val expected6 = spark.sql("SELECT * FROM variant_explode_outer(parse_json('1'))")
    checkAnswer(actual6, expected6)
  }

  test("variant_explode_outer - lateral join") {
    withView("variant_table") {
      sql(
        """
          |CREATE VIEW variant_table(id, v) AS
          |SELECT id, parse_json(v) AS v FROM VALUES
          |(0, '["hello", "world"]'), (1, '{"a": true, "b": 3.14}'),
          |(2, '[]'), (3, '{}'),
          |(4, NULL), (5, '1')
          |AS t(id, v)
          |""".stripMargin)
      val variantTable = spark.table("variant_table")

      checkAnswer(
        variantTable.as("t1").lateralJoin(
          spark.tvf.variant_explode_outer($"v".outer()).as("t")
        ).select($"t1.id", $"t.*"),
        sql("SELECT t1.id, t.* FROM variant_table AS t1, LATERAL variant_explode_outer(v) AS t")
      )
    }
  }
}
