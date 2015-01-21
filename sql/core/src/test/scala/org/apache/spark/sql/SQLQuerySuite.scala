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

import java.util.TimeZone

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

/* Implicits */
import org.apache.spark.sql.TestData._
import org.apache.spark.sql.test.TestSQLContext._

class SQLQuerySuite extends QueryTest with BeforeAndAfterAll {
  // Make sure the tables are loaded.
  TestData

  var origZone: TimeZone = _
  override protected def beforeAll() {
    origZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  }

  override protected def afterAll() {
    TimeZone.setDefault(origZone)
  }

  test("SPARK-4625 support SORT BY in SimpleSQLParser & DSL") {
    checkAnswer(
      sql("SELECT a FROM testData2 SORT BY a"),
      Seq(1, 1, 2 ,2 ,3 ,3).map(Row(_))
    )
  }

  test("grouping on nested fields") {
    jsonRDD(sparkContext.parallelize("""{"nested": {"attribute": 1}, "value": 2}""" :: Nil))
     .registerTempTable("rows")

    checkAnswer(
      sql(
        """
          |select attribute, sum(cnt)
          |from (
          |  select nested.attribute, count(*) as cnt
          |  from rows
          |  group by nested.attribute) a
          |group by attribute
        """.stripMargin),
      Row(1, 1) :: Nil)
  }

  test("SPARK-3176 Added Parser of SQL ABS()") {
    checkAnswer(
      sql("SELECT ABS(-1.3)"),
      Row(1.3))
    checkAnswer(
      sql("SELECT ABS(0.0)"),
      Row(0.0))
    checkAnswer(
      sql("SELECT ABS(2.5)"),
      Row(2.5))
  }

  test("aggregation with codegen") {
    val originalValue = conf.codegenEnabled
    setConf(SQLConf.CODEGEN_ENABLED, "true")
    sql("SELECT key FROM testData GROUP BY key").collect()
    setConf(SQLConf.CODEGEN_ENABLED, originalValue.toString)
  }

  test("SPARK-3176 Added Parser of SQL LAST()") {
    checkAnswer(
      sql("SELECT LAST(n) FROM lowerCaseData"),
      Row(4))
  }

  test("SPARK-2041 column name equals tablename") {
    checkAnswer(
      sql("SELECT tableName FROM tableName"),
      Row("test"))
  }

  test("SQRT") {
    checkAnswer(
      sql("SELECT SQRT(key) FROM testData"),
      (1 to 100).map(x => Row(math.sqrt(x.toDouble))).toSeq
    )
  }

  test("SQRT with automatic string casts") {
    checkAnswer(
      sql("SELECT SQRT(CAST(key AS STRING)) FROM testData"),
      (1 to 100).map(x => Row(math.sqrt(x.toDouble))).toSeq
    )
  }

  test("SPARK-2407 Added Parser of SQL SUBSTR()") {
    checkAnswer(
      sql("SELECT substr(tableName, 1, 2) FROM tableName"),
      Row("te"))
    checkAnswer(
      sql("SELECT substr(tableName, 3) FROM tableName"),
      Row("st"))
    checkAnswer(
      sql("SELECT substring(tableName, 1, 2) FROM tableName"),
      Row("te"))
    checkAnswer(
      sql("SELECT substring(tableName, 3) FROM tableName"),
      Row("st"))
  }

  test("SPARK-3173 Timestamp support in the parser") {
    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time=CAST('1970-01-01 00:00:00.001' AS TIMESTAMP)"),
      Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.001")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='1970-01-01 00:00:00.001'"),
      Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.001")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE '1970-01-01 00:00:00.001'=time"),
      Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.001")))

    checkAnswer(sql(
      """SELECT time FROM timestamps WHERE time<'1970-01-01 00:00:00.003'
          AND time>'1970-01-01 00:00:00.001'"""),
      Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.002")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time IN ('1970-01-01 00:00:00.001','1970-01-01 00:00:00.002')"),
      Seq(Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.001")),
        Row(java.sql.Timestamp.valueOf("1970-01-01 00:00:00.002"))))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='123'"),
      Nil)
  }

  test("index into array") {
    checkAnswer(
      sql("SELECT data, data[0], data[0] + data[1], data[0 + 1] FROM arrayData"),
      arrayData.map(d => Row(d.data, d.data(0), d.data(0) + d.data(1), d.data(1))).collect())
  }

  test("left semi greater than predicate") {
    checkAnswer(
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.a >= y.a + 2"),
      Seq(Row(3,1), Row(3,2))
    )
  }

  test("index into array of arrays") {
    checkAnswer(
      sql(
        "SELECT nestedData, nestedData[0][0], nestedData[0][0] + nestedData[0][1] FROM arrayData"),
      arrayData.map(d =>
        Row(d.nestedData,
         d.nestedData(0)(0),
         d.nestedData(0)(0) + d.nestedData(0)(1))).collect().toSeq)
  }

  test("agg") {
    checkAnswer(
      sql("SELECT a, SUM(b) FROM testData2 GROUP BY a"),
      Seq(Row(1,3), Row(2,3), Row(3,3)))
  }

  test("aggregates with nulls") {
    checkAnswer(
      sql("SELECT MIN(a), MAX(a), AVG(a), SUM(a), COUNT(a) FROM nullInts"),
      Row(1, 3, 2, 6, 3)
    )
  }

  test("select *") {
    checkAnswer(
      sql("SELECT * FROM testData"),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      sql("SELECT value FROM testData WHERE key = 1"),
      Row("1"))
  }

  def sortTest() = {
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b ASC"),
      Seq(Row(1,1), Row(1,2), Row(2,1), Row(2,2), Row(3,1), Row(3,2)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b DESC"),
      Seq(Row(1,2), Row(1,1), Row(2,2), Row(2,1), Row(3,2), Row(3,1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b DESC"),
      Seq(Row(3,2), Row(3,1), Row(2,2), Row(2,1), Row(1,2), Row(1,1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b ASC"),
      Seq(Row(3,1), Row(3,2), Row(2,1), Row(2,2), Row(1,1), Row(1,2)))

    checkAnswer(
      sql("SELECT b FROM binaryData ORDER BY a ASC"),
      (1 to 5).map(Row(_)))

    checkAnswer(
      sql("SELECT b FROM binaryData ORDER BY a DESC"),
      (1 to 5).map(Row(_)).toSeq.reverse)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] ASC"),
      arrayData.collect().sortBy(_.data(0)).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] DESC"),
      arrayData.collect().sortBy(_.data(0)).reverse.map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] ASC"),
      mapData.collect().sortBy(_.data(1)).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] DESC"),
      mapData.collect().sortBy(_.data(1)).reverse.map(Row.fromTuple).toSeq)
  }

  test("sorting") {
    val before = conf.externalSortEnabled
    setConf(SQLConf.EXTERNAL_SORT, "false")
    sortTest()
    setConf(SQLConf.EXTERNAL_SORT, before.toString)
  }

  test("external sorting") {
    val before = conf.externalSortEnabled
    setConf(SQLConf.EXTERNAL_SORT, "true")
    sortTest()
    setConf(SQLConf.EXTERNAL_SORT, before.toString)
  }

  test("limit") {
    checkAnswer(
      sql("SELECT * FROM testData LIMIT 10"),
      testData.take(10).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData LIMIT 1"),
      arrayData.collect().take(1).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData LIMIT 1"),
      mapData.collect().take(1).map(Row.fromTuple).toSeq)
  }

  test("from follow multiple brackets") {
    checkAnswer(sql(
      "select key from ((select * from testData limit 1) union all (select * from testData limit 1)) x limit 1"),
      Row(1)
    )

    checkAnswer(sql(
      "select key from (select * from testData) x limit 1"),
      Row(1)
    )

    checkAnswer(sql(
      "select key from (select * from testData limit 1 union all select * from testData limit 1) x limit 1"),
      Row(1)
    )
  }

  test("average") {
    checkAnswer(
      sql("SELECT AVG(a) FROM testData2"),
      Row(2.0))
  }

  test("average overflow") {
    checkAnswer(
      sql("SELECT AVG(a),b FROM largeAndSmallInts group by b"),
      Seq(Row(2147483645.0,1), Row(2.0,2)))
  }

  test("count") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM testData2"),
      Row(testData2.count()))
  }

  test("count distinct") {
    checkAnswer(
      sql("SELECT COUNT(DISTINCT b) FROM testData2"),
      Row(2))
  }

  test("approximate count distinct") {
    checkAnswer(
      sql("SELECT APPROXIMATE COUNT(DISTINCT a) FROM testData2"),
      Row(3))
  }

  test("approximate count distinct with user provided standard deviation") {
    checkAnswer(
      sql("SELECT APPROXIMATE(0.04) COUNT(DISTINCT a) FROM testData2"),
      Row(3))
  }

  test("null count") {
    checkAnswer(
      sql("SELECT a, COUNT(b) FROM testData3 GROUP BY a"),
      Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(
      sql("SELECT COUNT(a), COUNT(b), COUNT(1), COUNT(DISTINCT a), COUNT(DISTINCT b) FROM testData3"),
      Row(2, 1, 2, 2, 1))
  }

  test("inner join where, one match per row") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData WHERE n = N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }

  test("inner join ON, one match per row") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON n = N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }

  test("inner join, where, multiple matches") {
    checkAnswer(
      sql("""
        |SELECT * FROM
        |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
        |  (SELECT * FROM testData2 WHERE a = 1) y
        |WHERE x.a = y.a""".stripMargin),
      Row(1,1,1,1) ::
      Row(1,1,1,2) ::
      Row(1,2,1,1) ::
      Row(1,2,1,2) :: Nil)
  }

  test("inner join, no matches") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
          |  (SELECT * FROM testData2 WHERE a = 2) y
          |WHERE x.a = y.a""".stripMargin),
      Nil)
  }

  test("big inner join, 4 matches per row") {


    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) x JOIN
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) y
          |WHERE x.key = y.key""".stripMargin),
      testData.flatMap(
        row => Seq.fill(16)(Row.merge(row, row))).collect().toSeq)
  }

  ignore("cartesian product join") {
    checkAnswer(
      testData3.join(testData3),
      Row(1, null, 1, null) ::
      Row(1, null, 2, 2) ::
      Row(2, 2, 1, null) ::
      Row(2, 2, 2, 2) :: Nil)
  }

  test("left outer join") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData LEFT OUTER JOIN lowerCaseData ON n = N"),
      Row(1, "A", 1, "a") ::
      Row(2, "B", 2, "b") ::
      Row(3, "C", 3, "c") ::
      Row(4, "D", 4, "d") ::
      Row(5, "E", null, null) ::
      Row(6, "F", null, null) :: Nil)
  }

  test("right outer join") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData RIGHT OUTER JOIN upperCaseData ON n = N"),
      Row(1, "a", 1, "A") ::
      Row(2, "b", 2, "B") ::
      Row(3, "c", 3, "C") ::
      Row(4, "d", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("full outer join") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM upperCaseData WHERE N <= 4) leftTable FULL OUTER JOIN
          |  (SELECT * FROM upperCaseData WHERE N >= 3) rightTable
          |    ON leftTable.N = rightTable.N
        """.stripMargin),
      Row(1, "A", null, null) ::
      Row(2, "B", null, null) ::
      Row(3, "C", 3, "C") ::
      Row (4, "D", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("SPARK-3349 partitioning after limit") {
    sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n DESC")
      .limit(2)
      .registerTempTable("subset1")
    sql("SELECT DISTINCT n FROM lowerCaseData")
      .limit(2)
      .registerTempTable("subset2")
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INNER JOIN subset1 ON subset1.n = lowerCaseData.n"),
      Row(3, "c", 3) ::
      Row(4, "d", 4) :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INNER JOIN subset2 ON subset2.n = lowerCaseData.n"),
      Row(1, "a", 1) ::
      Row(2, "b", 2) :: Nil)
  }

  test("mixed-case keywords") {
    checkAnswer(
      sql(
        """
          |SeleCT * from
          |  (select * from upperCaseData WherE N <= 4) leftTable fuLL OUtER joiN
          |  (sElEcT * FROM upperCaseData whERe N >= 3) rightTable
          |    oN leftTable.N = rightTable.N
        """.stripMargin),
      Row(1, "A", null, null) ::
      Row(2, "B", null, null) ::
      Row(3, "C", 3, "C") ::
      Row(4, "D", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("select with table name as qualifier") {
    checkAnswer(
      sql("SELECT testData.value FROM testData WHERE testData.key = 1"),
      Row("1"))
  }

  test("inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }

  test("qualified select with inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT upperCaseData.N, upperCaseData.L FROM upperCaseData JOIN lowerCaseData " +
        "ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        Row(1, "A"),
        Row(2, "B"),
        Row(3, "C"),
        Row(4, "D")))
  }

  test("system function upper()") {
    checkAnswer(
      sql("SELECT n,UPPER(l) FROM lowerCaseData"),
      Seq(
        Row(1, "A"),
        Row(2, "B"),
        Row(3, "C"),
        Row(4, "D")))

    checkAnswer(
      sql("SELECT n, UPPER(s) FROM nullStrings"),
      Seq(
        Row(1, "ABC"),
        Row(2, "ABC"),
        Row(3, null)))
  }

  test("system function lower()") {
    checkAnswer(
      sql("SELECT N,LOWER(L) FROM upperCaseData"),
      Seq(
        Row(1, "a"),
        Row(2, "b"),
        Row(3, "c"),
        Row(4, "d"),
        Row(5, "e"),
        Row(6, "f")))

    checkAnswer(
      sql("SELECT n, LOWER(s) FROM nullStrings"),
      Seq(
        Row(1, "abc"),
        Row(2, "abc"),
        Row(3, null)))
  }

  test("UNION") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION SELECT * FROM upperCaseData"),
      Row(1, "A") :: Row(1, "a") :: Row(2, "B") :: Row(2, "b") :: Row(3, "C") :: Row(3, "c") ::
      Row(4, "D") :: Row(4, "d") :: Row(5, "E") :: Row(6, "F") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION SELECT * FROM lowerCaseData"),
      Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION ALL SELECT * FROM lowerCaseData"),
      Row(1, "a") :: Row(1, "a") :: Row(2, "b") :: Row(2, "b") :: Row(3, "c") :: Row(3, "c") ::
      Row(4, "d") :: Row(4, "d") :: Nil)
  }

  test("UNION with column mismatches") {
    // Column name mismatches are allowed.
    checkAnswer(
      sql("SELECT n,l FROM lowerCaseData UNION SELECT N as x1, L as x2 FROM upperCaseData"),
      Row(1, "A") :: Row(1, "a") :: Row(2, "B") :: Row(2, "b") :: Row(3, "C") :: Row(3, "c") ::
      Row(4, "D") :: Row(4, "d") :: Row(5, "E") :: Row(6, "F") :: Nil)
    // Column type mismatches are not allowed, forcing a type coercion.
    checkAnswer(
      sql("SELECT n FROM lowerCaseData UNION SELECT L FROM upperCaseData"),
      ("1" :: "2" :: "3" :: "4" :: "A" :: "B" :: "C" :: "D" :: "E" :: "F" :: Nil).map(Row(_)))
    // Column type mismatches where a coercion is not possible, in this case between integer
    // and array types, trigger a TreeNodeException.
    intercept[TreeNodeException[_]] {
      sql("SELECT data FROM arrayData UNION SELECT 1 FROM arrayData").collect()
    }
  }

  test("EXCEPT") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData EXCEPT SELECT * FROM upperCaseData"),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData EXCEPT SELECT * FROM lowerCaseData"), Nil)
    checkAnswer(
      sql("SELECT * FROM upperCaseData EXCEPT SELECT * FROM upperCaseData"), Nil)
  }

  test("INTERSECT") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INTERSECT SELECT * FROM lowerCaseData"),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INTERSECT SELECT * FROM upperCaseData"), Nil)
  }

  test("SET commands semantics using sql()") {
    conf.clear()
    val testKey = "test.key.0"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(sql("SET").collect().size == 0)

    // "set key=val"
    sql(s"SET $testKey=$testVal")
    checkAnswer(
      sql("SET"),
      Row(s"$testKey=$testVal")
    )

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    checkAnswer(
      sql("set"),
      Seq(
        Row(s"$testKey=$testVal"),
        Row(s"${testKey + testKey}=${testVal + testVal}"))
    )

    // "set key"
    checkAnswer(
      sql(s"SET $testKey"),
      Row(s"$testKey=$testVal")
    )
    checkAnswer(
      sql(s"SET $nonexistentKey"),
      Row(s"$nonexistentKey=<undefined>")
    )
    conf.clear()
  }

  test("apply schema") {
    val schema1 = StructType(
      StructField("f1", IntegerType, false) ::
      StructField("f2", StringType, false) ::
      StructField("f3", BooleanType, false) ::
      StructField("f4", IntegerType, true) :: Nil)

    val rowRDD1 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(values(0).toInt, values(1), values(2).toBoolean, v4)
    }

    val schemaRDD1 = applySchema(rowRDD1, schema1)
    schemaRDD1.registerTempTable("applySchema1")
    checkAnswer(
      sql("SELECT * FROM applySchema1"),
      Row(1, "A1", true, null) ::
      Row(2, "B2", false, null) ::
      Row(3, "C3", true, null) ::
      Row(4, "D4", true, 2147483644) :: Nil)

    checkAnswer(
      sql("SELECT f1, f4 FROM applySchema1"),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, 2147483644) :: Nil)

    val schema2 = StructType(
      StructField("f1", StructType(
        StructField("f11", IntegerType, false) ::
        StructField("f12", BooleanType, false) :: Nil), false) ::
      StructField("f2", MapType(StringType, IntegerType, true), false) :: Nil)

    val rowRDD2 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(Row(values(0).toInt, values(2).toBoolean), Map(values(1) -> v4))
    }

    val schemaRDD2 = applySchema(rowRDD2, schema2)
    schemaRDD2.registerTempTable("applySchema2")
    checkAnswer(
      sql("SELECT * FROM applySchema2"),
      Row(Row(1, true), Map("A1" -> null)) ::
      Row(Row(2, false), Map("B2" -> null)) ::
      Row(Row(3, true), Map("C3" -> null)) ::
      Row(Row(4, true), Map("D4" -> 2147483644)) :: Nil)

    checkAnswer(
      sql("SELECT f1.f11, f2['D4'] FROM applySchema2"),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, 2147483644) :: Nil)

    // The value of a MapType column can be a mutable map.
    val rowRDD3 = unparsedStrings.map { r =>
      val values = r.split(",").map(_.trim)
      val v4 = try values(3).toInt catch {
        case _: NumberFormatException => null
      }
      Row(Row(values(0).toInt, values(2).toBoolean), scala.collection.mutable.Map(values(1) -> v4))
    }

    val schemaRDD3 = applySchema(rowRDD3, schema2)
    schemaRDD3.registerTempTable("applySchema3")

    checkAnswer(
      sql("SELECT f1.f11, f2['D4'] FROM applySchema3"),
      Row(1, null) ::
      Row(2, null) ::
      Row(3, null) ::
      Row(4, 2147483644) :: Nil)
  }

  test("SPARK-3423 BETWEEN") {
    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 5 and 7"),
      Seq(Row(5, "5"), Row(6, "6"), Row(7, "7"))
    )

    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 7 and 7"),
      Row(7, "7")
    )

    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 9 and 7"),
      Nil
    )
  }

  test("cast boolean to string") {
    // TODO Ensure true/false string letter casing is consistent with Hive in all cases.
    checkAnswer(
      sql("SELECT CAST(TRUE AS STRING), CAST(FALSE AS STRING) FROM testData LIMIT 1"),
      Row("true", "false"))
  }

  test("metadata is propagated correctly") {
    val person = sql("SELECT * FROM person")
    val schema = person.schema
    val docKey = "doc"
    val docValue = "first name"
    val metadata = new MetadataBuilder()
      .putString(docKey, docValue)
      .build()
    val schemaWithMeta = new StructType(Array(
      schema("id"), schema("name").copy(metadata = metadata), schema("age")))
    val personWithMeta = applySchema(person, schemaWithMeta)
    def validateMetadata(rdd: SchemaRDD): Unit = {
      assert(rdd.schema("name").metadata.getString(docKey) == docValue)
    }
    personWithMeta.registerTempTable("personWithMeta")
    validateMetadata(personWithMeta.select('name))
    validateMetadata(personWithMeta.select("name".attr))
    validateMetadata(personWithMeta.select('id, 'name))
    validateMetadata(sql("SELECT * FROM personWithMeta"))
    validateMetadata(sql("SELECT id, name FROM personWithMeta"))
    validateMetadata(sql("SELECT * FROM personWithMeta JOIN salary ON id = personId"))
    validateMetadata(sql("SELECT name, salary FROM personWithMeta JOIN salary ON id = personId"))
  }

  test("SPARK-3371 Renaming a function expression with group by gives error") {
    udf.register("len", (s: String) => s.length)
    checkAnswer(
      sql("SELECT len(value) as temp FROM testData WHERE key = 1 group by len(value)"),
      Row(1))
  }

  test("SPARK-3813 CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END") {
    checkAnswer(
      sql("SELECT CASE key WHEN 1 THEN 1 ELSE 0 END FROM testData WHERE key = 1 group by key"),
      Row(1))
  }

  test("SPARK-3813 CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END") {
    checkAnswer(
      sql("SELECT CASE WHEN key = 1 THEN 1 ELSE 2 END FROM testData WHERE key = 1 group by key"),
      Row(1))
  }

  test("throw errors for non-aggregate attributes with aggregation") {
    def checkAggregation(query: String, isInvalidQuery: Boolean = true) {
      val logicalPlan = sql(query).queryExecution.logical

      if (isInvalidQuery) {
        val e = intercept[TreeNodeException[LogicalPlan]](sql(query).queryExecution.analyzed)
        assert(
          e.getMessage.startsWith("Expression not in GROUP BY"),
          "Non-aggregate attribute(s) not detected\n" + logicalPlan)
      } else {
        // Should not throw
        sql(query).queryExecution.analyzed
      }
    }

    checkAggregation("SELECT key, COUNT(*) FROM testData")
    checkAggregation("SELECT COUNT(key), COUNT(*) FROM testData", false)

    checkAggregation("SELECT value, COUNT(*) FROM testData GROUP BY key")
    checkAggregation("SELECT COUNT(value), SUM(key) FROM testData GROUP BY key", false)

    checkAggregation("SELECT key + 2, COUNT(*) FROM testData GROUP BY key + 1")
    checkAggregation("SELECT key + 1 + 1, COUNT(*) FROM testData GROUP BY key + 1", false)
  }

  test("Test to check we can use Long.MinValue") {
    checkAnswer(
      sql(s"SELECT ${Long.MinValue} FROM testData ORDER BY key LIMIT 1"), Row(Long.MinValue)
    )

    checkAnswer(
      sql(s"SELECT key FROM testData WHERE key > ${Long.MinValue}"),
      (1 to 100).map(Row(_)).toSeq
    )
  }

  test("Floating point number format") {
    checkAnswer(
      sql("SELECT 0.3"), Row(0.3)
    )

    checkAnswer(
      sql("SELECT -0.8"), Row(-0.8)
    )

    checkAnswer(
      sql("SELECT .5"), Row(0.5)
    )

    checkAnswer(
      sql("SELECT -.18"), Row(-0.18)
    )
  }

  test("Auto cast integer type") {
    checkAnswer(
      sql(s"SELECT ${Int.MaxValue + 1L}"), Row(Int.MaxValue + 1L)
    )

    checkAnswer(
      sql(s"SELECT ${Int.MinValue - 1L}"), Row(Int.MinValue - 1L)
    )

    checkAnswer(
      sql("SELECT 9223372036854775808"), Row(new java.math.BigDecimal("9223372036854775808"))
    )

    checkAnswer(
      sql("SELECT -9223372036854775809"), Row(new java.math.BigDecimal("-9223372036854775809"))
    )
  }

  test("Test to check we can apply sign to expression") {

    checkAnswer(
      sql("SELECT -100"), Row(-100)
    )

    checkAnswer(
      sql("SELECT +230"), Row(230)
    )

    checkAnswer(
      sql("SELECT -5.2"), Row(-5.2)
    )

    checkAnswer(
      sql("SELECT +6.8"), Row(6.8)
    )

    checkAnswer(
      sql("SELECT -key FROM testData WHERE key = 2"), Row(-2)
    )

    checkAnswer(
      sql("SELECT +key FROM testData WHERE key = 3"), Row(3)
    )

    checkAnswer(
      sql("SELECT -(key + 1) FROM testData WHERE key = 1"), Row(-2)
    )

    checkAnswer(
      sql("SELECT - key + 1 FROM testData WHERE key = 10"), Row(-9)
    )

    checkAnswer(
      sql("SELECT +(key + 5) FROM testData WHERE key = 5"), Row(10)
    )

    checkAnswer(
      sql("SELECT -MAX(key) FROM testData"), Row(-100)
    )

    checkAnswer(
      sql("SELECT +MAX(key) FROM testData"), Row(100)
    )

    checkAnswer(
      sql("SELECT - (-10)"), Row(10)
    )

    checkAnswer(
      sql("SELECT + (-key) FROM testData WHERE key = 32"), Row(-32)
    )

    checkAnswer(
      sql("SELECT - (+Max(key)) FROM testData"), Row(-100)
    )

    checkAnswer(
      sql("SELECT - - 3"), Row(3)
    )

    checkAnswer(
      sql("SELECT - + 20"), Row(-20)
    )

    checkAnswer(
      sql("SELEcT - + 45"), Row(-45)
    )

    checkAnswer(
      sql("SELECT + + 100"), Row(100)
    )

    checkAnswer(
      sql("SELECT - - Max(key) FROM testData"), Row(100)
    )

    checkAnswer(
      sql("SELECT + - key FROM testData WHERE key = 33"), Row(-33)
    )
  }

  test("Multiple join") {
    checkAnswer(
      sql(
        """SELECT a.key, b.key, c.key
          |FROM testData a
          |JOIN testData b ON a.key = b.key
          |JOIN testData c ON a.key = c.key
        """.stripMargin),
      (1 to 100).map(i => Row(i, i, i)))
  }

  test("SPARK-3483 Special chars in column names") {
    val data = sparkContext.parallelize(Seq("""{"key?number1": "value1", "key.number2": "value2"}"""))
    jsonRDD(data).registerTempTable("records")
    sql("SELECT `key?number1` FROM records")
  }

  test("SPARK-3814 Support Bitwise & operator") {
    checkAnswer(sql("SELECT key&1 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise | operator") {
    checkAnswer(sql("SELECT key|0 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise ^ operator") {
    checkAnswer(sql("SELECT key^0 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise ~ operator") {
    checkAnswer(sql("SELECT ~key FROM testData WHERE key = 1 "), Row(-2))
  }

  test("SPARK-4120 Join of multiple tables does not work in SparkSQL") {
    checkAnswer(
      sql(
        """SELECT a.key, b.key, c.key
          |FROM testData a,testData b,testData c
          |where a.key = b.key and a.key = c.key
        """.stripMargin),
      (1 to 100).map(i => Row(i, i, i)))
  }

  test("SPARK-4154 Query does not work if it has 'not between' in Spark SQL and HQL") {
    checkAnswer(sql("SELECT key FROM testData WHERE key not between 0 and 10 order by key"),
        (11 to 100).map(i => Row(i)))
  }

  test("SPARK-4207 Query which has syntax like 'not like' is not working in Spark SQL") {
    checkAnswer(sql("SELECT key FROM testData WHERE value not like '100%' order by key"),
        (1 to 99).map(i => Row(i)))
  }

  test("SPARK-4322 Grouping field with struct field as sub expression") {
    jsonRDD(sparkContext.makeRDD("""{"a": {"b": [{"c": 1}]}}""" :: Nil)).registerTempTable("data")
    checkAnswer(sql("SELECT a.b[0].c FROM data GROUP BY a.b[0].c"), Row(1))
    dropTempTable("data")

    jsonRDD(sparkContext.makeRDD("""{"a": {"b": 1}}""" :: Nil)).registerTempTable("data")
    checkAnswer(sql("SELECT a.b + 1 FROM data GROUP BY a.b + 1"), Row(2))
    dropTempTable("data")
  }

  test("SPARK-4432 Fix attribute reference resolution error when using ORDER BY") {
    checkAnswer(
      sql("SELECT a + b FROM testData2 ORDER BY a"),
      Seq(2, 3, 3 ,4 ,4 ,5).map(Row(_))
    )
  }

  test("oder by asc by default when not specify ascending and descending") {
    checkAnswer(
      sql("SELECT a, b FROM testData2 ORDER BY a desc, b"),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2,2), Row(1, 1), Row(1, 2))
    )
  }

  test("Supporting relational operator '<=>' in Spark SQL") {
    val nullCheckData1 = TestData(1,"1") :: TestData(2,null) :: Nil
    val rdd1 = sparkContext.parallelize((0 to 1).map(i => nullCheckData1(i)))
    rdd1.registerTempTable("nulldata1")
    val nullCheckData2 = TestData(1,"1") :: TestData(2,null) :: Nil
    val rdd2 = sparkContext.parallelize((0 to 1).map(i => nullCheckData2(i)))
    rdd2.registerTempTable("nulldata2")
    checkAnswer(sql("SELECT nulldata1.key FROM nulldata1 join " +
      "nulldata2 on nulldata1.value <=> nulldata2.value"),
        (1 to 2).map(i => Row(i)))
  }

  test("Multi-column COUNT(DISTINCT ...)") {
    val data = TestData(1,"val_1") :: TestData(2,"val_2") :: Nil
    val rdd = sparkContext.parallelize((0 to 1).map(i => data(i)))
    rdd.registerTempTable("distinctData")
    checkAnswer(sql("SELECT COUNT(DISTINCT key,value) FROM distinctData"), Row(2))
  }
}
