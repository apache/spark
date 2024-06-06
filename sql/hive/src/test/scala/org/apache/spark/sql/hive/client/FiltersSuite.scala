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

package org.apache.spark.sql.hive.client

import java.sql.Date
import java.util.Collections

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A set of tests for the filter conversion logic used when pushing partition pruning into the
 * metastore
 */
class FiltersSuite extends SparkFunSuite with PlanTest {
  private val shim = new Shim_v2_0

  private val testTable = new org.apache.hadoop.hive.ql.metadata.Table("default", "test")
  private val varCharCol = new FieldSchema()
  varCharCol.setName("varchar")
  varCharCol.setType(serdeConstants.VARCHAR_TYPE_NAME)
  testTable.setPartCols(Collections.singletonList(varCharCol))

  filterTest("string filter",
    (a("stringcol", StringType) > Literal("test")) :: Nil,
    "stringcol > \"test\"")

  filterTest("string filter backwards",
    (Literal("test") > a("stringcol", StringType)) :: Nil,
    "\"test\" > stringcol")

  filterTest("int filter",
    (a("intcol", IntegerType) === Literal(1)) :: Nil,
    "intcol = 1")

  filterTest("int filter backwards",
    (Literal(1) === a("intcol", IntegerType)) :: Nil,
    "1 = intcol")

  filterTest("int and string filter",
    (Literal(1) === a("intcol", IntegerType)) :: (Literal("a") === a("strcol", IntegerType)) :: Nil,
    "1 = intcol and \"a\" = strcol")

  filterTest("date filter",
    (a("datecol", DateType) === Literal(Date.valueOf("2019-01-01"))) :: Nil,
    "datecol = \"2019-01-01\"")

  filterTest("date filter with IN predicate",
    (a("datecol", DateType) in
      (Literal(Date.valueOf("2019-01-01")), Literal(Date.valueOf("2019-01-07")))) :: Nil,
    "(datecol = \"2019-01-01\" or datecol = \"2019-01-07\")")

  filterTest("date and string filter",
    (Literal(Date.valueOf("2019-01-01")) === a("datecol", DateType)) ::
      (Literal("a") === a("strcol", IntegerType)) :: Nil,
    "\"2019-01-01\" = datecol and \"a\" = strcol")

  filterTest("date filter with null",
    (a("datecol", DateType) ===  Literal(null)) :: Nil,
    "")

  filterTest("string filter with InSet predicate",
    InSet(a("strcol", StringType), Set("1", "2").map(s => UTF8String.fromString(s))) :: Nil,
    "(strcol = \"1\" or strcol = \"2\")")

  filterTest("skip varchar",
    (Literal("") === a("varchar", StringType)) :: Nil,
    "")

  filterTest("SPARK-19912 String literals should be escaped for Hive metastore partition pruning",
    (a("stringcol", StringType) === Literal("p1\" and q=\"q1")) ::
      (Literal("p2\" and q=\"q2") === a("stringcol", StringType)) :: Nil,
    """stringcol = 'p1" and q="q1' and 'p2" and q="q2' = stringcol""")

  filterTest("SPARK-24879 null literals should be ignored for IN constructs",
    (a("intcol", IntegerType) in (Literal(1), Literal(null))) :: Nil,
    "(intcol = 1)")

  filterTest("NOT: int and string filters",
    (a("intcol", IntegerType) =!= Literal(1)) :: (Literal("a") =!= a("strcol", IntegerType)) :: Nil,
    """intcol != 1 and "a" != strcol""")

  filterTest("NOT: date filter",
    (a("datecol", DateType) =!= Literal(Date.valueOf("2019-01-01"))) :: Nil,
    "datecol != \"2019-01-01\"")

  filterTest("not-in, string filter",
    (Not(In(a("strcol", StringType), Seq(Literal("a"), Literal("b"))))) :: Nil,
    """(strcol != "a" and strcol != "b")""")

  filterTest("not-in, string filter with null",
    (Not(In(a("strcol", StringType), Seq(Literal("a"), Literal("b"), Literal(null))))) :: Nil,
    "")

  filterTest("not-in, date filter",
    (Not(In(a("datecol", DateType),
      Seq(Literal(Date.valueOf("2021-01-01")), Literal(Date.valueOf("2021-01-02")))))) :: Nil,
    """(datecol != "2021-01-01" and datecol != "2021-01-02")""")

  filterTest("not-in, date filter with null",
    (Not(In(a("datecol", DateType),
      Seq(Literal(Date.valueOf("2021-01-01")), Literal(Date.valueOf("2021-01-02")),
        Literal(null))))) :: Nil,
    "")

  filterTest("not-inset, string filter",
    (Not(InSet(a("strcol", StringType), Set(Literal("a").eval(), Literal("b").eval())))) :: Nil,
    """(strcol != "a" and strcol != "b")""")

  filterTest("not-inset, string filter with null",
    (Not(InSet(a("strcol", StringType),
      Set(Literal("a").eval(), Literal("b").eval(), Literal(null).eval())))) :: Nil,
    "")

  filterTest("not-inset, date filter",
    (Not(InSet(a("datecol", DateType),
      Set(Literal(Date.valueOf("2020-01-01")).eval(),
        Literal(Date.valueOf("2020-01-02")).eval())))) :: Nil,
    """(datecol != "2020-01-01" and datecol != "2020-01-02")""")

  filterTest("not-inset, date filter with null",
    (Not(InSet(a("datecol", DateType),
      Set(Literal(Date.valueOf("2020-01-01")).eval(),
        Literal(Date.valueOf("2020-01-02")).eval(),
        Literal(null).eval())))) :: Nil,
    "")

  // Applying the predicate `x IN (NULL)` should return an empty set, but since this optimization
  // will be applied by Catalyst, this filter converter does not need to account for this.
  filterTest("SPARK-24879 IN predicates with only NULLs will not cause a NPE",
    (a("intcol", IntegerType) in Literal(null)) :: Nil,
    "")

  filterTest("typecast null literals should not be pushed down in simple predicates",
    (a("intcol", IntegerType) === Literal(null, IntegerType)) :: Nil,
    "")

  private def filterTest(name: String, filters: Seq[Expression], result: String) = {
    test(name) {
      withSQLConf(SQLConf.ADVANCED_PARTITION_PREDICATE_PUSHDOWN.key -> "true") {
        val converted = shim.convertFilters(testTable, filters)
        if (converted != result) {
          fail(s"Expected ${filters.mkString(",")} to convert to '$result' but got '$converted'")
        }
      }
    }
  }

  test("turn on/off ADVANCED_PARTITION_PREDICATE_PUSHDOWN") {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.ADVANCED_PARTITION_PREDICATE_PUSHDOWN.key -> enabled.toString) {
        val filters =
          (Literal(1) === a("intcol", IntegerType) ||
            Literal(2) === a("intcol", IntegerType)) :: Nil
        val converted = shim.convertFilters(testTable, filters)
        if (enabled) {
          assert(converted == "(1 = intcol or 2 = intcol)")
        } else {
          assert(converted.isEmpty)
        }
      }
    }
  }

  test("SPARK-33416: Avoid Hive metastore stack overflow when InSet predicate have many values") {
    def checkConverted(inSet: InSet, result: String): Unit = {
      assert(shim.convertFilters(testTable, inSet :: Nil) == result)
    }

    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD.key -> "15") {
      checkConverted(
        InSet(a("intcol", IntegerType),
          Range(1, 20).map(s => Literal(s).eval(EmptyRow)).toSet),
        "(intcol >= 1 and intcol <= 19)")

      checkConverted(
        InSet(a("stringcol", StringType),
          Range(1, 20).map(s => Literal(s.toString).eval(EmptyRow)).toSet),
        "(stringcol >= \"1\" and stringcol <= \"9\")")

      checkConverted(
        InSet(a("intcol", IntegerType).cast(LongType),
          Range(1, 20).map(s => Literal(s.toLong).eval(EmptyRow)).toSet),
        "(intcol >= 1 and intcol <= 19)")

      checkConverted(
        InSet(a("doublecol", DoubleType),
          Range(1, 20).map(s => Literal(s.toDouble).eval(EmptyRow)).toSet),
        "")

      checkConverted(
        InSet(a("datecol", DateType),
          Range(1, 20).map(d => Literal(d, DateType).eval(EmptyRow)).toSet),
        "(datecol >= \"1970-01-02\" and datecol <= \"1970-01-20\")")
    }
  }

  test("SPARK-34515: Fix NPE if InSet contains null value during getPartitionsByFilter") {
    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD.key -> "2") {
      val filter = InSet(a("p", IntegerType), Set(null, 1, 2))
      val converted = shim.convertFilters(testTable, Seq(filter))
      assert(converted == "(p >= 1 and p <= 2)")
    }
  }

  test("Don't push not inset if it's values exceeds the threshold") {
    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD.key -> "2") {
      val filter = Not(InSet(a("p", IntegerType), Set(1, 2, 3)))
      val converted = shim.convertFilters(testTable, Seq(filter))
      assert(converted.isEmpty)
    }
  }

  test("SPARK-34538: Skip InSet null value during push filter to Hive metastore") {
    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD.key -> "3") {
      val intFilter = InSet(a("p", IntegerType), Set(null, 1, 2))
      val intConverted = shim.convertFilters(testTable, Seq(intFilter))
      assert(intConverted == "(p = 1 or p = 2)")
    }

    withSQLConf(SQLConf.HIVE_METASTORE_PARTITION_PRUNING_INSET_THRESHOLD.key -> "3") {
      val dateFilter = InSet(a("p", DateType), Set(null,
        Literal(Date.valueOf("2020-01-01")).eval(), Literal(Date.valueOf("2021-01-01")).eval()))
      val dateConverted = shim.convertFilters(testTable, Seq(dateFilter))
      assert(dateConverted == "(p = \"2020-01-01\" or p = \"2021-01-01\")")
    }
  }

  private def a(name: String, dataType: DataType) = AttributeReference(name, dataType)()
}
