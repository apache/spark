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

package org.apache.spark.sql.hive.execution

import java.io.{DataInput, DataOutput, File, PrintWriter}
import java.util.{ArrayList, Arrays, Properties}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.hive.ql.udf.{UDAFPercentile, UDFType}
import org.apache.hadoop.hive.ql.udf.generic._
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.{AbstractSerDe, SerDeStats}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.{LongWritable, Writable}

import org.apache.spark.{SparkFiles, TestUtils}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.command.FunctionsCommand
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

case class Fields(f1: Int, f2: Int, f3: Int, f4: Int, f5: Int)

// Case classes for the custom UDF's.
case class IntegerCaseClass(i: Int)
case class ListListIntCaseClass(lli: Seq[(Int, Int, Int)])
case class StringCaseClass(s: String)
case class ListStringCaseClass(l: Seq[String])

/**
 * A test suite for Hive custom UDFs.
 */
class HiveUDFSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  import spark.udf
  import spark.implicits._

  test("spark sql udf test that returns a struct") {
    udf.register("getStruct", (_: Int) => Fields(1, 2, 3, 4, 5))
    assert(sql(
      """
        |SELECT getStruct(1).f1,
        |       getStruct(1).f2,
        |       getStruct(1).f3,
        |       getStruct(1).f4,
        |       getStruct(1).f5 FROM src LIMIT 1
      """.stripMargin).head() === Row(1, 2, 3, 4, 5))
  }

  test("SPARK-4785 When called with arguments referring column fields, PMOD throws NPE") {
    checkAnswer(
      sql("SELECT PMOD(CAST(key as INT), 10) FROM src LIMIT 1"),
      Row(8)
    )
  }

  test("hive struct udf") {
    withTable("hiveUDFTestTable") {
      sql(
          """
          |CREATE TABLE hiveUDFTestTable (
          |   pair STRUCT<id: INT, value: INT>
          |)
          |PARTITIONED BY (partition STRING)
          |ROW FORMAT SERDE '%s'
          |STORED AS SEQUENCEFILE
        """.
            stripMargin.format(classOf[PairSerDe].getName))

      val location = Utils.getSparkClassLoader.getResource("data/files/testUDF").getFile
      sql(s"""
        ALTER TABLE hiveUDFTestTable
        ADD IF NOT EXISTS PARTITION(partition='testUDF')
        LOCATION '$location'""")

      sql(s"CREATE TEMPORARY FUNCTION testUDF AS '${classOf[PairUDF].getName}'")
      sql("SELECT testUDF(pair) FROM hiveUDFTestTable")
      sql("DROP TEMPORARY FUNCTION IF EXISTS testUDF")
    }
  }

  test("Max/Min on named_struct") {
    checkAnswer(sql(
      """
        |SELECT max(named_struct(
        |           "key", key,
        |           "value", value)).value FROM src
      """.stripMargin), Seq(Row("val_498")))
    checkAnswer(sql(
      """
        |SELECT min(named_struct(
        |           "key", key,
        |           "value", value)).value FROM src
      """.stripMargin), Seq(Row("val_0")))

    // nested struct cases
    checkAnswer(sql(
      """
        |SELECT max(named_struct(
        |           "key", named_struct(
                            "key", key,
                            "value", value),
        |           "value", value)).value FROM src
      """.stripMargin), Seq(Row("val_498")))
    checkAnswer(sql(
      """
        |SELECT min(named_struct(
        |           "key", named_struct(
                           "key", key,
                           "value", value),
        |           "value", value)).value FROM src
      """.stripMargin), Seq(Row("val_0")))
  }

  test("SPARK-6409 UDAF Average test") {
    sql(s"CREATE TEMPORARY FUNCTION test_avg AS '${classOf[GenericUDAFAverage].getName}'")
    checkAnswer(
      sql("SELECT test_avg(1), test_avg(substr(value,5)) FROM src"),
      Seq(Row(1.0, 260.182)))
    sql("DROP TEMPORARY FUNCTION IF EXISTS test_avg")
    hiveContext.reset()
  }

  test("SPARK-2693 udaf aggregates test") {
    checkAnswer(sql("SELECT percentile(key, 1) FROM src LIMIT 1"),
      sql("SELECT max(key) FROM src").collect().toSeq)

    checkAnswer(sql("SELECT percentile(key, array(1, 1)) FROM src LIMIT 1"),
      sql("SELECT array(max(key), max(key)) FROM src").collect().toSeq)
  }

  test("Generic UDAF aggregates") {

    checkAnswer(sql(
     """
       |SELECT percentile_approx(2, 0.99999),
       |       sum(distinct 1),
       |       count(distinct 1,2,3,4) FROM src LIMIT 1
     """.stripMargin), sql("SELECT 2, 1, 1 FROM src LIMIT 1").collect().toSeq)

    checkAnswer(sql(
      """
        |SELECT ceiling(percentile_approx(distinct key, 0.99999)),
        |       count(distinct key),
        |       sum(distinct key),
        |       count(distinct 1),
        |       sum(distinct 1),
        |       sum(1) FROM src LIMIT 1
      """.stripMargin),
      sql(
        """
          |SELECT max(key),
          |       count(distinct key),
          |       sum(distinct key),
          |       1, 1, sum(1) FROM src LIMIT 1
        """.stripMargin).collect().toSeq)

    checkAnswer(sql(
      """
        |SELECT ceiling(percentile_approx(distinct key, 0.9 + 0.09999)),
        |       count(distinct key), sum(distinct key),
        |       count(distinct 1), sum(distinct 1),
        |       sum(1) FROM src LIMIT 1
      """.stripMargin),
      sql("SELECT max(key), count(distinct key), sum(distinct key), 1, 1, sum(1) FROM src LIMIT 1")
        .collect().toSeq)

    checkAnswer(sql("SELECT ceiling(percentile_approx(key, 0.99999D)) FROM src LIMIT 1"),
      sql("SELECT max(key) FROM src LIMIT 1").collect().toSeq)

    checkAnswer(sql("SELECT percentile_approx(100.0D, array(0.9D, 0.9D)) FROM src LIMIT 1"),
      sql("SELECT array(100, 100) FROM src LIMIT 1").collect().toSeq)
  }

  test("UDFIntegerToString") {
    val testData = spark.sparkContext.parallelize(
      IntegerCaseClass(1) :: IntegerCaseClass(2) :: Nil).toDF()
    testData.createOrReplaceTempView("integerTable")

    val udfName = classOf[UDFIntegerToString].getName
    sql(s"CREATE TEMPORARY FUNCTION testUDFIntegerToString AS '$udfName'")
    checkAnswer(
      sql("SELECT testUDFIntegerToString(i) FROM integerTable"),
      Seq(Row("1"), Row("2")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFIntegerToString")

    hiveContext.reset()
  }

  test("UDFToListString") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFToListString AS '${classOf[UDFToListString].getName}'")
    checkAnswer(
      sql("SELECT testUDFToListString(s) FROM inputTable"),
      Seq(Row(Seq("data1", "data2", "data3"))))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToListString")
    hiveContext.reset()
  }

  test("UDFToListInt") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFToListInt AS '${classOf[UDFToListInt].getName}'")
    checkAnswer(
      sql("SELECT testUDFToListInt(s) FROM inputTable"),
      Seq(Row(Seq(1, 2, 3))))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToListInt")
    hiveContext.reset()
  }

  test("UDFToStringIntMap") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFToStringIntMap " +
      s"AS '${classOf[UDFToStringIntMap].getName}'")
    checkAnswer(
      sql("SELECT testUDFToStringIntMap(s) FROM inputTable"),
      Seq(Row(Map("key1" -> 1, "key2" -> 2, "key3" -> 3))))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToStringIntMap")
    hiveContext.reset()
  }

  test("UDFToIntIntMap") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFToIntIntMap " +
      s"AS '${classOf[UDFToIntIntMap].getName}'")
    checkAnswer(
      sql("SELECT testUDFToIntIntMap(s) FROM inputTable"),
      Seq(Row(Map(1 -> 1, 2 -> 1, 3 -> 1))))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToIntIntMap")
    hiveContext.reset()
  }

  test("UDFToListMapStringListInt") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFToListMapStringListInt " +
      s"AS '${classOf[UDFToListMapStringListInt].getName}'")
    checkAnswer(
      sql("SELECT testUDFToListMapStringListInt(s) FROM inputTable"),
      Seq(Row(Seq(Map("a" -> Seq(1, 2), "b" -> Seq(3, 4))))))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToListMapStringListInt")
    hiveContext.reset()
  }

  test("UDFRawList") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFRawList " +
      s"AS '${classOf[UDFRawList].getName}'")
    val err = intercept[AnalysisException](sql("SELECT testUDFRawList(s) FROM inputTable"))
    assert(err.getMessage.contains(
      "Raw list type in java is unsupported because Spark cannot infer the element type."))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFRawList")
    hiveContext.reset()
  }

  test("UDFRawMap") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFRawMap " +
      s"AS '${classOf[UDFRawMap].getName}'")
    val err = intercept[AnalysisException](sql("SELECT testUDFRawMap(s) FROM inputTable"))
    assert(err.getMessage.contains(
      "Raw map type in java is unsupported because Spark cannot infer key and value types."))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFRawMap")
    hiveContext.reset()
  }

  test("UDFWildcardList") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    testData.createOrReplaceTempView("inputTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFWildcardList " +
      s"AS '${classOf[UDFWildcardList].getName}'")
    val err = intercept[AnalysisException](sql("SELECT testUDFWildcardList(s) FROM inputTable"))
    assert(err.getMessage.contains(
      "Collection types with wildcards (e.g. List<?> or Map<?, ?>) are unsupported " +
        "because Spark cannot infer the data type for these type parameters."))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFWildcardList")
    hiveContext.reset()
  }

  test("UDFListListInt") {
    val testData = spark.sparkContext.parallelize(
      ListListIntCaseClass(Nil) ::
      ListListIntCaseClass(Seq((1, 2, 3))) ::
      ListListIntCaseClass(Seq((4, 5, 6), (7, 8, 9))) :: Nil).toDF()
    testData.createOrReplaceTempView("listListIntTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFListListInt AS '${classOf[UDFListListInt].getName}'")
    checkAnswer(
      sql("SELECT testUDFListListInt(lli) FROM listListIntTable"),
      Seq(Row(0), Row(2), Row(13)))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFListListInt")

    hiveContext.reset()
  }

  test("UDFListString") {
    val testData = spark.sparkContext.parallelize(
      ListStringCaseClass(Seq("a", "b", "c")) ::
      ListStringCaseClass(Seq("d", "e")) :: Nil).toDF()
    testData.createOrReplaceTempView("listStringTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFListString AS '${classOf[UDFListString].getName}'")
    checkAnswer(
      sql("SELECT testUDFListString(l) FROM listStringTable"),
      Seq(Row("a,b,c"), Row("d,e")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFListString")

    hiveContext.reset()
  }

  test("UDFStringString") {
    val testData = spark.sparkContext.parallelize(
      StringCaseClass("world") :: StringCaseClass("goodbye") :: Nil).toDF()
    testData.createOrReplaceTempView("stringTable")

    sql(s"CREATE TEMPORARY FUNCTION testStringStringUDF AS '${classOf[UDFStringString].getName}'")
    checkAnswer(
      sql("SELECT testStringStringUDF(\"hello\", s) FROM stringTable"),
      Seq(Row("hello world"), Row("hello goodbye")))

    checkAnswer(
      sql("SELECT testStringStringUDF(\"\", testStringStringUDF(\"hello\", s)) FROM stringTable"),
      Seq(Row(" hello world"), Row(" hello goodbye")))

    sql("DROP TEMPORARY FUNCTION IF EXISTS testStringStringUDF")

    hiveContext.reset()
  }

  test("UDFTwoListList") {
    val testData = spark.sparkContext.parallelize(
      ListListIntCaseClass(Nil) ::
      ListListIntCaseClass(Seq((1, 2, 3))) ::
      ListListIntCaseClass(Seq((4, 5, 6), (7, 8, 9))) ::
      Nil).toDF()
    testData.createOrReplaceTempView("TwoListTable")

    sql(s"CREATE TEMPORARY FUNCTION testUDFTwoListList AS '${classOf[UDFTwoListList].getName}'")
    checkAnswer(
      sql("SELECT testUDFTwoListList(lli, lli) FROM TwoListTable"),
      Seq(Row("0, 0"), Row("2, 2"), Row("13, 13")))
    sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFTwoListList")

    hiveContext.reset()
  }

  test("non-deterministic children of UDF") {
    withUserDefinedFunction("testStringStringUDF" -> true, "testGenericUDFHash" -> true) {
      // HiveSimpleUDF
      sql(s"CREATE TEMPORARY FUNCTION testStringStringUDF AS '${classOf[UDFStringString].getName}'")
      val df1 = sql("SELECT testStringStringUDF(rand(), \"hello\")")
      assert(!df1.logicalPlan.asInstanceOf[Project].projectList.forall(_.deterministic))

      // HiveGenericUDF
      sql(s"CREATE TEMPORARY FUNCTION testGenericUDFHash AS '${classOf[GenericUDFHash].getName}'")
      val df2 = sql("SELECT testGenericUDFHash(rand())")
      assert(!df2.logicalPlan.asInstanceOf[Project].projectList.forall(_.deterministic))
    }
  }

  test("Hive UDFs with insufficient number of input arguments should trigger an analysis error") {
    withTempView("testUDF") {
      Seq((1, 2)).toDF("a", "b").createOrReplaceTempView("testUDF")

      def testErrorMsgForFunc(funcName: String, className: String): Unit = {
        withUserDefinedFunction(funcName -> true) {
          sql(s"CREATE TEMPORARY FUNCTION $funcName AS '$className'")
          val message = intercept[AnalysisException] {
            sql(s"SELECT $funcName() FROM testUDF")
          }.getMessage
          assert(message.contains(s"No handler for UDF/UDAF/UDTF '$className'"))
        }
      }

      // HiveSimpleUDF
      testErrorMsgForFunc("testUDFTwoListList", classOf[UDFTwoListList].getName)

      // HiveGenericUDF
      testErrorMsgForFunc("testUDFAnd", classOf[GenericUDFOPAnd].getName)

      // Hive UDAF
      testErrorMsgForFunc("testUDAFPercentile", classOf[UDAFPercentile].getName)

      // AbstractGenericUDAFResolver
      testErrorMsgForFunc("testUDAFAverage", classOf[GenericUDAFAverage].getName)

      // AbstractGenericUDAFResolver
      testErrorMsgForFunc("testUDTFExplode", classOf[GenericUDTFExplode].getName)
    }
  }

  test("Hive UDF in group by") {
    withTempView("tab1") {
      Seq(Tuple1(1451400761)).toDF("test_date").createOrReplaceTempView("tab1")
      sql(s"CREATE TEMPORARY FUNCTION testUDFToDate AS '${classOf[GenericUDFToDate].getName}'")
      val count = sql("select testUDFToDate(timestamp_seconds(test_date))" +
        " from tab1 group by testUDFToDate(timestamp_seconds(test_date))").count()
      sql("DROP TEMPORARY FUNCTION IF EXISTS testUDFToDate")
      assert(count == 1)
    }
  }

  test("SPARK-11522 select input_file_name from non-parquet table") {

    withTempDir { tempDir =>

      // EXTERNAL OpenCSVSerde table pointing to LOCATION

      val file1 = new File(tempDir + "/data1")
      Utils.tryWithResource(new PrintWriter(file1)) { writer =>
        writer.write("1,2")
      }

      val file2 = new File(tempDir + "/data2")
      Utils.tryWithResource(new PrintWriter(file2)) { writer =>
        writer.write("1,2")
      }

      sql(
        s"""CREATE EXTERNAL TABLE csv_table(page_id INT, impressions INT)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
          \"separatorChar\" = \",\",
          \"quoteChar\"     = \"\\\"\",
          \"escapeChar\"    = \"\\\\\")
        LOCATION '${tempDir.toURI}'
      """)

      val answer1 =
        sql("SELECT input_file_name() FROM csv_table").head().getString(0)
      assert(answer1.contains("data1") || answer1.contains("data2"))

      val count1 = sql("SELECT input_file_name() FROM csv_table").distinct().count()
      assert(count1 == 2)
      sql("DROP TABLE csv_table")

      // EXTERNAL pointing to LOCATION

      sql(
        s"""CREATE EXTERNAL TABLE external_t5 (c1 int, c2 int)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION '${tempDir.toURI}'
      """)

      val answer2 =
        sql("SELECT input_file_name() as file FROM external_t5").head().getString(0)
      assert(answer1.contains("data1") || answer1.contains("data2"))

      val count2 = sql("SELECT input_file_name() as file FROM external_t5").distinct().count
      assert(count2 == 2)
      sql("DROP TABLE external_t5")
    }

    withTempDir { tempDir =>

      // External parquet pointing to LOCATION

      val parquetLocation = s"${tempDir.toURI}/external_parquet"
      sql("SELECT 1, 2").write.parquet(parquetLocation)

      sql(
        s"""CREATE EXTERNAL TABLE external_parquet(c1 int, c2 int)
        STORED AS PARQUET
        LOCATION '$parquetLocation'
      """)

      val answer3 =
        sql("SELECT input_file_name() as file FROM external_parquet").head().getString(0)
      assert(answer3.contains("external_parquet"))

      val count3 = sql("SELECT input_file_name() as file FROM external_parquet").distinct().count
      assert(count3 == 1)
      sql("DROP TABLE external_parquet")
    }

    // Non-External parquet pointing to /tmp/...
    sql("CREATE TABLE parquet_tmp STORED AS parquet AS SELECT 1, 2")

    val answer4 =
      sql("SELECT input_file_name() as file FROM parquet_tmp").head().getString(0)
    assert(answer4.contains("parquet_tmp"))

    val count4 = sql("SELECT input_file_name() as file FROM parquet_tmp").distinct().count
    assert(count4 == 1)
    sql("DROP TABLE parquet_tmp")
  }

  test("Hive Stateful UDF") {
    withUserDefinedFunction("statefulUDF" -> true, "statelessUDF" -> true) {
      sql(s"CREATE TEMPORARY FUNCTION statefulUDF AS '${classOf[StatefulUDF].getName}'")
      sql(s"CREATE TEMPORARY FUNCTION statelessUDF AS '${classOf[StatelessUDF].getName}'")
      val testData = spark.range(10).repartition(1)

      // Expected Max(s) is 10 as statefulUDF returns the sequence number starting from 1.
      checkAnswer(testData.selectExpr("statefulUDF() as s").agg(max($"s")), Row(10))

      // Expected Max(s) is 5 as statefulUDF returns the sequence number starting from 1,
      // and the data is evenly distributed into 2 partitions.
      checkAnswer(testData.repartition(2)
        .selectExpr("statefulUDF() as s").agg(max($"s")), Row(5))

      // Expected Max(s) is 1, as stateless UDF is deterministic and foldable and replaced
      // by constant 1 by ConstantFolding optimizer.
      checkAnswer(testData.selectExpr("statelessUDF() as s").agg(max($"s")), Row(1))
    }
  }

  test("Show persistent functions") {
    val testData = spark.sparkContext.parallelize(StringCaseClass("") :: Nil).toDF()
    withTempView("inputTable") {
      testData.createOrReplaceTempView("inputTable")
      withUserDefinedFunction("testUDFToListInt" -> false) {
        val numFunc = spark.catalog.listFunctions().count()
        sql(s"CREATE FUNCTION testUDFToListInt AS '${classOf[UDFToListInt].getName}'")
        assert(spark.catalog.listFunctions().count() == numFunc + 1)
        checkAnswer(
          sql("SELECT testUDFToListInt(s) FROM inputTable"),
          Seq(Row(Seq(1, 2, 3))))
        assert(sql("show functions").count() ==
          numFunc + FunctionsCommand.virtualOperators.size + 1)
        assert(spark.catalog.listFunctions().count() == numFunc + 1)

        withDatabase("db2") {
          sql("CREATE DATABASE db2")
          sql(s"CREATE FUNCTION db2.testUDFToListInt AS '${classOf[UDFToListInt].getName}'")
          checkAnswer(
            sql("SHOW FUNCTIONS IN db2 LIKE 'testUDF*'"),
            Seq(Row("db2.testudftolistint")))
        }
      }
    }
  }

  test("Temp function has dots in the names") {
    withUserDefinedFunction("test_avg" -> false, "`default.test_avg`" -> true) {
      sql(s"CREATE FUNCTION test_avg AS '${classOf[GenericUDAFAverage].getName}'")
      checkAnswer(sql("SELECT test_avg(1)"), Row(1.0))
      // temp function containing dots in the name
      spark.udf.register("default.test_avg", () => { Math.random() + 2})
      assert(sql("SELECT `default.test_avg`()").head().getDouble(0) >= 2.0)
      checkAnswer(sql("SELECT test_avg(1)"), Row(1.0))
    }
  }

  test("Call the function registered in the not-current database") {
    Seq("true", "false").foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        withDatabase("dAtABaSe1") {
          sql("CREATE DATABASE dAtABaSe1")
          withUserDefinedFunction("dAtABaSe1.test_avg" -> false) {
            sql(s"CREATE FUNCTION dAtABaSe1.test_avg AS '${classOf[GenericUDAFAverage].getName}'")
            checkAnswer(sql("SELECT dAtABaSe1.test_avg(1)"), Row(1.0))
          }
          val message = intercept[AnalysisException] {
            sql("SELECT dAtABaSe1.unknownFunc(1)")
          }.getMessage
          assert(message.contains("Undefined function: 'unknownFunc'") &&
            message.contains("nor a permanent function registered in the database 'dAtABaSe1'"))
        }
      }
    }
  }

  test("UDTF") {
    withUserDefinedFunction("udtf_count2" -> true) {
      sql(s"ADD JAR ${hiveContext.getHiveFile("TestUDTF.jar").getCanonicalPath}")
      // The function source code can be found at:
      // https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF
      sql(
        """
          |CREATE TEMPORARY FUNCTION udtf_count2
          |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
        """.stripMargin)

      checkAnswer(
        sql("SELECT key, cc FROM src LATERAL VIEW udtf_count2(value) dd AS cc"),
        Row(97, 500) :: Row(97, 500) :: Nil)

      checkAnswer(
        sql("SELECT udtf_count2(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
    }
  }

  test("permanent UDTF") {
    withUserDefinedFunction("udtf_count_temp" -> false) {
      sql(
        s"""
           |CREATE FUNCTION udtf_count_temp
           |AS 'org.apache.spark.sql.hive.execution.GenericUDTFCount2'
           |USING JAR '${hiveContext.getHiveFile("TestUDTF.jar").toURI}'
        """.stripMargin)

      checkAnswer(
        sql("SELECT key, cc FROM src LATERAL VIEW udtf_count_temp(value) dd AS cc"),
        Row(97, 500) :: Row(97, 500) :: Nil)

      checkAnswer(
        sql("SELECT udtf_count_temp(a) FROM (SELECT 1 AS a FROM src LIMIT 3) t"),
        Row(3) :: Row(3) :: Nil)
    }
  }

  test("SPARK-25768 constant argument expecting Hive UDF") {
    withTempView("inputTable") {
      spark.range(10).createOrReplaceTempView("inputTable")
      withUserDefinedFunction("testGenericUDAFPercentileApprox" -> false) {
        val numFunc = spark.catalog.listFunctions().count()
        sql(s"CREATE FUNCTION testGenericUDAFPercentileApprox AS '" +
          s"${classOf[GenericUDAFPercentileApprox].getName}'")
        checkAnswer(
          sql("SELECT testGenericUDAFPercentileApprox(id, 0.5) FROM inputTable"),
          Seq(Row(4.0)))
      }
    }
  }
  test("SPARK-28012 Hive UDF supports struct type foldable expression") {
    withUserDefinedFunction("testUDFStructType" -> false) {
      // Simulate a hive udf that supports struct parameters
      sql("CREATE FUNCTION testUDFStructType AS '" +
        s"${classOf[GenericUDFArray].getName}'")
      checkAnswer(
        sql("SELECT testUDFStructType(named_struct('name', 'xx', 'value', 1))[0].value"),
        Seq(Row(1)))
    }
  }

  test("SPARK-32877: add test for Hive UDF complex decimal type") {
    withUserDefinedFunction("testArraySum" -> false) {
      sql(s"CREATE FUNCTION testArraySum AS '${classOf[ArraySumUDF].getName}'")
      checkAnswer(
        sql("SELECT testArraySum(array(1, 1.1, 1.2))"),
        Seq(Row(3.3)))

      val msg = intercept[AnalysisException] {
        sql("SELECT testArraySum(1)")
      }.getMessage
      assert(msg.contains(s"No handler for UDF/UDAF/UDTF '${classOf[ArraySumUDF].getName}'"))

      val msg2 = intercept[AnalysisException] {
        sql("SELECT testArraySum(1, 2)")
      }.getMessage
      assert(msg2.contains(s"No handler for UDF/UDAF/UDTF '${classOf[ArraySumUDF].getName}'"))
    }
  }

  test("SPARK-35236: CREATE FUNCTION should take an archive in USING clause") {
    withTempDir { dir =>
      withUserDefinedFunction("testListFiles1" -> false) {
        val text1 = File.createTempFile("test1_", ".txt", dir)
        val json1 = File.createTempFile("test1_", ".json", dir)
        val zipFile1 = File.createTempFile("test1_", ".zip", dir)
        TestUtils.createJar(Seq(text1, json1), zipFile1)

        sql(s"CREATE FUNCTION testListFiles1 AS '${classOf[ListFiles].getName}' " +
          s"USING ARCHIVE '${zipFile1.getAbsolutePath}'")
        val df1 = sql(s"SELECT testListFiles1('${SparkFiles.get(zipFile1.getName)}')")
        val fileList1 =
          df1.collect().map(_.getList[String](0)).head.asScala.filter(_ != "META-INF")

        assert(fileList1.length === 2)
        assert(fileList1.contains(text1.getName))
        assert(fileList1.contains(json1.getName))
      }

      // Test for file#alias style archive registration.
      withUserDefinedFunction("testListFiles2" -> false) {
        val text2 = File.createTempFile("test2_", ".txt", dir)
        val json2 = File.createTempFile("test2_", ".json", dir)
        val csv2 = File.createTempFile("test2", ".csv", dir)
        val zipFile2 = File.createTempFile("test2_", ".zip", dir)
        TestUtils.createJar(Seq(text2, json2, csv2), zipFile2)

        sql(s"CREATE FUNCTION testListFiles2 AS '${classOf[ListFiles].getName}' " +
          s"USING ARCHIVE '${zipFile2.getAbsolutePath}#foo'")
        val df2 = sql(s"SELECT testListFiles2('${SparkFiles.get("foo")}')")
        val fileList2 =
          df2.collect().map(_.getList[String](0)).head.asScala.filter(_ != "META-INF")

        assert(fileList2.length === 3)
        assert(fileList2.contains(text2.getName))
        assert(fileList2.contains(json2.getName))
        assert(fileList2.contains(csv2.getName))
      }
    }
  }
}

class TestPair(x: Int, y: Int) extends Writable with Serializable {
  def this() = this(0, 0)
  var entry: (Int, Int) = (x, y)

  override def write(output: DataOutput): Unit = {
    output.writeInt(entry._1)
    output.writeInt(entry._2)
  }

  override def readFields(input: DataInput): Unit = {
    val x = input.readInt()
    val y = input.readInt()
    entry = (x, y)
  }
}

class PairSerDe extends AbstractSerDe {
  override def initialize(p1: Configuration, p2: Properties): Unit = {}

  override def getObjectInspector: ObjectInspector = {
    ObjectInspectorFactory
      .getStandardStructObjectInspector(
        Arrays.asList("pair"),
        Arrays.asList(ObjectInspectorFactory.getStandardStructObjectInspector(
          Arrays.asList("id", "value"),
          Arrays.asList(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                        PrimitiveObjectInspectorFactory.javaIntObjectInspector))
    ))
  }

  override def getSerializedClass: Class[_ <: Writable] = classOf[TestPair]

  override def getSerDeStats: SerDeStats = null

  override def serialize(p1: scala.Any, p2: ObjectInspector): Writable = null

  override def deserialize(value: Writable): AnyRef = {
    val pair = value.asInstanceOf[TestPair]

    val row = new ArrayList[ArrayList[AnyRef]]
    row.add(new ArrayList[AnyRef](2))
    row.get(0).add(Integer.valueOf(pair.entry._1))
    row.get(0).add(Integer.valueOf(pair.entry._2))

    row
  }
}

class PairUDF extends GenericUDF {
  override def initialize(p1: Array[ObjectInspector]): ObjectInspector =
    ObjectInspectorFactory.getStandardStructObjectInspector(
      Arrays.asList("id", "value"),
      Arrays.asList(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
                    PrimitiveObjectInspectorFactory.javaIntObjectInspector)
  )

  override def evaluate(args: Array[DeferredObject]): AnyRef = {
    Integer.valueOf(args(0).get.asInstanceOf[TestPair].entry._2)
  }

  override def getDisplayString(p1: Array[String]): String = ""
}

@UDFType(stateful = true)
class StatefulUDF extends UDF {
  private val result = new LongWritable(0)

  def evaluate(): LongWritable = {
    result.set(result.get() + 1)
    result
  }
}

class StatelessUDF extends UDF {
  private val result = new LongWritable(0)

  def evaluate(): LongWritable = {
    result.set(result.get() + 1)
    result
  }
}

class ArraySumUDF extends UDF {
  def evaluate(values: java.util.List[java.lang.Double]): java.lang.Double = {
    var r = 0d
    for (v <- values.asScala) {
      r += v
    }
    r
  }
}

class ListFiles extends UDF {
  import java.util.{ArrayList, Arrays, List => JList}
  def evaluate(path: String): JList[String] = {
    val fileArray = new File(path).list()
    if (fileArray != null) Arrays.asList(fileArray: _*) else new ArrayList[String]()
  }
}
