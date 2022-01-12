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

import java.sql.Timestamp
import java.time.{Duration, Period}
import java.time.temporal.ChronoUnit

import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.DateTimeConstants
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DayTimeIntervalType._
import org.apache.spark.sql.types.YearMonthIntervalType._
import org.apache.spark.tags.SlowHiveTest
import org.apache.spark.unsafe.types.CalendarInterval

@SlowHiveTest
class HiveScriptTransformationSuite extends BaseScriptTransformationSuite with TestHiveSingleton {
  import testImplicits._

  import ScriptTransformationIOSchema._

  override protected def defaultSerDe(): String = "hive-serde"

  override def createScriptTransformationExec(
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec = {
    HiveScriptTransformationExec(
      script = script,
      output = output,
      child = child,
      ioschema = ioschema
    )
  }

  private val hiveIOSchema: ScriptTransformationIOSchema = {
    defaultIOSchema.copy(
      inputSerdeClass = Some(classOf[LazySimpleSerDe].getCanonicalName),
      outputSerdeClass = Some(classOf[LazySimpleSerDe].getCanonicalName)
    )
  }

  test("cat with LazySimpleSerDe") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    checkAnswer(
      rowsDf,
      (child: SparkPlan) => createScriptTransformationExec(
        script = "cat",
        output = Seq(AttributeReference("a", StringType)()),
        child = child,
        ioschema = hiveIOSchema
      ),
      rowsDf.collect())
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("script transformation should not swallow errors from upstream operators (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[TestFailedException] {
      checkAnswer(
        rowsDf,
        (child: SparkPlan) => createScriptTransformationExec(
          script = "cat",
          output = Seq(AttributeReference("a", StringType)()),
          child = ExceptionInjectingOperator(child),
          ioschema = hiveIOSchema
        ),
        rowsDf.collect())
    }
    assert(e.getMessage().contains("intentional exception"))
    // Before SPARK-25158, uncaughtExceptionHandler will catch IllegalArgumentException
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-14400 script transformation should fail for bad script command (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")

    val e = intercept[SparkException] {
      val plan =
        createScriptTransformationExec(
          script = "some_non_existent_command",
          output = Seq(AttributeReference("a", StringType)()),
          child = rowsDf.queryExecution.sparkPlan,
          ioschema = hiveIOSchema)
      SparkPlanTest.executePlan(plan, hiveContext)
    }
    assert(e.getMessage.contains("Subprocess exited with status"))
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-24339 verify the result after pruning the unused columns (hive serde)") {
    val rowsDf = Seq(
      ("Bob", 16, 176),
      ("Alice", 32, 164),
      ("David", 60, 192),
      ("Amy", 24, 180)
    ).toDF("name", "age", "height")

    checkAnswer(
      rowsDf,
      (child: SparkPlan) => createScriptTransformationExec(
        script = "cat",
        output = Seq(AttributeReference("name", StringType)()),
        child = child,
        ioschema = hiveIOSchema
      ),
      rowsDf.select("name").collect())
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-30973: TRANSFORM should wait for the termination of the script (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[SparkException] {
      val plan =
        createScriptTransformationExec(
          script = "some_non_existent_command",
          output = Seq(AttributeReference("a", StringType)()),
          child = rowsDf.queryExecution.sparkPlan,
          ioschema = hiveIOSchema)
      SparkPlanTest.executePlan(plan, hiveContext)
    }
    assert(e.getMessage.contains("Subprocess exited with status"))
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-32388: TRANSFORM should handle schema less correctly (hive serde)") {
    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
      ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)
      df.createTempView("v")

      // In hive default serde mode, if we don't define output schema,
      // when output column size > 2 and don't specify serde,
      // it will choose take rest columns in second column as output schema
      // (key: String, value: String)
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  USING 'cat'
             |FROM v
        """.stripMargin),
        identity,
        df.select(
          'a.cast("string").as("key"),
          concat_ws("\t",
            'b.cast("string"),
            'c.cast("string"),
            'd.cast("string"),
            'e.cast("string")).as("value")).collect())

      // In hive default serde mode, if we don't define output schema,
      // when output column size > 2 and just specify serde,
      // it will choose take rest columns in second column as output schema
      // (key: String, value: String)
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t'
             |  )
             |  USING 'cat'
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t'
             |  )
             |FROM v
        """.stripMargin),
        identity,
        df.select(
          'a.cast("string").as("key"),
          'b.cast("string").as("value")).collect())


      // In hive default serde mode, if we don't define output schema,
      // when output column size > 2 and specify serde with
      // 'serialization.last.column.takes.rest=true',
      // it will choose take rest columns in second column as output schema
      // (key: String, value: String)
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'true'
             |  )
             |  USING 'cat'
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'true'
             |  )
             |FROM v
        """.stripMargin),
        identity,
        df.select(
          'a.cast("string").as("key"),
          concat_ws("\t",
            'b.cast("string"),
            'c.cast("string"),
            'd.cast("string"),
            'e.cast("string")).as("value")).collect())

      // In hive default serde mode, if we don't define output schema,
      // when output column size > 2 and specify serde
      // with 'serialization.last.column.takes.rest=false',
      // it will choose first two column as output schema (key: String, value: String)
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'false'
             |  )
             |  USING 'cat'
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'false'
             |  )
             |FROM v
        """.stripMargin),
        identity,
        df.select(
          'a.cast("string").as("key"),
          'b.cast("string").as("value")).collect())

      // In hive default serde mode, if we don't define output schema,
      // when output column size = 2 and specify serde, it will these two column as
      // output schema (key: String, value: String)
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b)
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'true'
             |  )
             |  USING 'cat'
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'true'
             |  )
             |FROM v
        """.stripMargin),
        identity,
        df.select(
          'a.cast("string").as("key"),
          'b.cast("string").as("value")).collect())

      // In hive default serde mode, if we don't define output schema,
      // when output column size < 2 and specify serde, it will return null for deficiency
      // output schema (key: String, value: String)
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a)
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'true'
             |  )
             |  USING 'cat'
             |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
             |  WITH SERDEPROPERTIES (
             |    'field.delim' = '\t',
             |    'serialization.last.column.takes.rest' = 'true'
             |  )
             |FROM v
        """.stripMargin),
        identity,
        df.select(
          'a.cast("string").as("key"),
          lit(null)).collect())
    }
  }

  testBasicInputDataTypesWith(hiveIOSchema, "hive serde")

  test("SPARK-32400: TRANSFORM supports complex data types type (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq(
        (1, "1", Array(0, 1, 2), Map("a" -> 1)),
        (2, "2", Array(3, 4, 5), Map("b" -> 2))
      ).toDF("a", "b", "c", "d")
        .select('a, 'b, 'c, 'd, struct('a, 'b).as("e"))
      df.createTempView("v")

      // Hive serde support ArrayType/MapType/StructType as input and output data type
      checkAnswer(
        df.select('c, 'd, 'e),
        (child: SparkPlan) => createScriptTransformationExec(
          script = "cat",
          output = Seq(
            AttributeReference("c", ArrayType(IntegerType))(),
            AttributeReference("d", MapType(StringType, IntegerType))(),
            AttributeReference("e", StructType(
              Seq(
                StructField("col1", IntegerType, false),
                StructField("col2", StringType, true))))()),
          child = child,
          ioschema = hiveIOSchema
        ),
        df.select('c, 'd, 'e).collect())
    }
  }

  test("SPARK-32400: TRANSFORM supports complex data types end to end (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq(
        (1, "1", Array(0, 1, 2), Map("a" -> 1)),
        (2, "2", Array(3, 4, 5), Map("b" -> 2))
      ).toDF("a", "b", "c", "d")
        .select('a, 'b, 'c, 'd, struct('a, 'b).as("e"))
      df.createTempView("v")

      // Hive serde support ArrayType/MapType/StructType as input and output data type
      val query = sql(
        """
          |SELECT TRANSFORM (c, d, e)
          |USING 'cat' AS (c array<int>, d map<string, int>, e struct<col1:int, col2:string>)
          |FROM v
        """.stripMargin)
      checkAnswer(query, identity, df.select('c, 'd, 'e).collect())
    }
  }

  test("SPARK-32400: TRANSFORM doesn't support CalendarIntervalType/UserDefinedType (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq(
        (1, new CalendarInterval(7, 1, 1000), new TestUDT.MyDenseVector(Array(1, 2, 3))),
        (1, new CalendarInterval(7, 1, 1000), new TestUDT.MyDenseVector(Array(1, 2, 3)))
      ).toDF("a", "b", "c")
      df.createTempView("v")

      val e1 = intercept[SparkException] {
        val plan = createScriptTransformationExec(
          script = "cat",
          output = Seq(
            AttributeReference("a", IntegerType)(),
            AttributeReference("b", CalendarIntervalType)()),
          child = df.select('a, 'b).queryExecution.sparkPlan,
          ioschema = hiveIOSchema)
        SparkPlanTest.executePlan(plan, hiveContext)
      }.getMessage
      assert(e1.contains("interval cannot be converted to Hive TypeInfo"))

      val e2 = intercept[SparkException] {
        val plan = createScriptTransformationExec(
          script = "cat",
          output = Seq(
            AttributeReference("a", IntegerType)(),
            AttributeReference("c", new TestUDT.MyDenseVectorUDT)()),
          child = df.select('a, 'c).queryExecution.sparkPlan,
          ioschema = hiveIOSchema)
        SparkPlanTest.executePlan(plan, hiveContext)
      }.getMessage
      assert(e2.contains("array<double> cannot be converted to Hive TypeInfo"))
    }
  }

  test("SPARK-32400: TRANSFORM doesn't support" +
    " CalendarIntervalType/UserDefinedType end to end (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq(
        (1, new CalendarInterval(7, 1, 1000), new TestUDT.MyDenseVector(Array(1, 2, 3))),
        (1, new CalendarInterval(7, 1, 1000), new TestUDT.MyDenseVector(Array(1, 2, 3)))
      ).toDF("a", "b", "c")
      df.createTempView("v")

      val e1 = intercept[SparkException] {
        sql(
          """
            |SELECT TRANSFORM(a, b) USING 'cat' AS (a, b)
            |FROM v
          """.stripMargin).collect()
      }.getMessage
      assert(e1.contains("interval cannot be converted to Hive TypeInfo"))

      val e2 = intercept[SparkException] {
        sql(
          """
            |SELECT TRANSFORM(a, c) USING 'cat' AS (a, c)
            |FROM v
          """.stripMargin).collect()
      }.getMessage
      assert(e2.contains("array<double> cannot be converted to Hive TypeInfo"))
    }
  }

  test("SPARK-32685: When use specified serde, filed.delim's default value is '\t'") {
    val query1 = sql(
      """
        |SELECT split(value, "\t") FROM (
        |SELECT TRANSFORM(a, b, c)
        |USING 'cat'
        |FROM (SELECT 1 AS a, 2 AS b, 3 AS c) t
        |) temp;
      """.stripMargin)
    checkAnswer(query1, identity, Row(Seq("2", "3")) :: Nil)

    val query2 = sql(
      """
        |SELECT split(value, "\t") FROM (
        |SELECT TRANSFORM(a, b, c)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |USING 'cat'
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |  WITH SERDEPROPERTIES (
        |   'serialization.last.column.takes.rest' = 'true'
        |  )
        |FROM (SELECT 1 AS a, 2 AS b, 3 AS c) t
        |) temp;
      """.stripMargin)
    checkAnswer(query2, identity, Row(Seq("2", "3")) :: Nil)

    val query3 = sql(
      """
        |SELECT split(value, "&") FROM (
        |SELECT TRANSFORM(a, b, c)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |  WITH SERDEPROPERTIES (
        |   'field.delim' = '&'
        |  )
        |USING 'cat'
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |  WITH SERDEPROPERTIES (
        |   'serialization.last.column.takes.rest' = 'true',
        |   'field.delim' = '&'
        |  )
        |FROM (SELECT 1 AS a, 2 AS b, 3 AS c) t
        |) temp;
      """.stripMargin)
    checkAnswer(query3, identity, Row(Seq("2", "3")) :: Nil)

    val query4 = sql(
      """
        |SELECT split(value, "&") FROM (
        |SELECT TRANSFORM(a, b, c)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |USING 'cat'
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |  WITH SERDEPROPERTIES (
        |   'serialization.last.column.takes.rest' = 'true',
        |   'field.delim' = '&'
        |  )
        |FROM (SELECT 1 AS a, 2 AS b, 3 AS c) t
        |) temp;
      """.stripMargin)
    checkAnswer(query4, identity, Row(null) :: Nil)
  }

  test("SPARK-32684: Script transform hive serde mode null format is same with hive as '\\N'") {
    val query1 = sql(
      """
        |SELECT TRANSFORM(null, null, null)
        |USING 'cat'
        |FROM (SELECT 1 AS a) t
      """.stripMargin)
    checkAnswer(query1, identity, Row(null, "\\N\t\\N") :: Nil)

    val query2 = sql(
      """
        |SELECT TRANSFORM(null, null, null)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |  WITH SERDEPROPERTIES (
        |   'field.delim' = ','
        |  )
        |USING 'cat' AS (a)
        |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        |  WITH SERDEPROPERTIES (
        |   'field.delim' = '&'
        |  )
        |FROM (SELECT 1 AS a) t
      """.stripMargin)
    checkAnswer(query2, identity, Row("\\N,\\N,\\N") :: Nil)

  }

  test("SPARK-34879, SPARK-35733: HiveInspectors supports all type of DayTimeIntervalType") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val schema = StructType(Seq(
        StructField("a", DayTimeIntervalType(DAY)),
        StructField("b", DayTimeIntervalType(DAY, HOUR)),
        StructField("c", DayTimeIntervalType(DAY, MINUTE)),
        StructField("d", DayTimeIntervalType(DAY, SECOND)),
        StructField("e", DayTimeIntervalType(HOUR)),
        StructField("f", DayTimeIntervalType(HOUR, MINUTE)),
        StructField("g", DayTimeIntervalType(HOUR, SECOND)),
        StructField("h", DayTimeIntervalType(MINUTE)),
        StructField("i", DayTimeIntervalType(MINUTE, SECOND)),
        StructField("j", DayTimeIntervalType(SECOND))
      ))
      val df = spark.createDataFrame(sparkContext.parallelize(Seq(
        Row(Duration.ofDays(1),
          Duration.ofHours(11),
          Duration.ofMinutes(1),
          Duration.ofSeconds(100).plusNanos(123456),
          Duration.of(Long.MaxValue, ChronoUnit.MICROS),
          Duration.ofDays(1),
          Duration.ofHours(11),
          Duration.ofMinutes(1),
          Duration.ofSeconds(100).plusNanos(123456),
          Duration.ofSeconds(Long.MaxValue / DateTimeConstants.MICROS_PER_SECOND)
        ))), schema)

      // Hive serde supports DayTimeIntervalType as input and output data type
      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          script = "cat",
          output = Seq(
            AttributeReference("a", DayTimeIntervalType(DAY))(),
            AttributeReference("b", DayTimeIntervalType(DAY, HOUR))(),
            AttributeReference("c", DayTimeIntervalType(DAY, MINUTE))(),
            AttributeReference("d", DayTimeIntervalType(DAY, SECOND))(),
            AttributeReference("e", DayTimeIntervalType(HOUR))(),
            AttributeReference("f", DayTimeIntervalType(HOUR, MINUTE))(),
            AttributeReference("g", DayTimeIntervalType(HOUR, SECOND))(),
            AttributeReference("h", DayTimeIntervalType(MINUTE))(),
            AttributeReference("i", DayTimeIntervalType(MINUTE, SECOND))(),
            AttributeReference("j", DayTimeIntervalType(SECOND))()),
          child = child,
          ioschema = hiveIOSchema),
        df.select($"a", $"b", $"c", $"d", $"e", $"f", $"g", $"h", $"i", $"j").collect())
    }
  }

  test("SPARK-34879, SPARK-35722: HiveInspectors supports all type of YearMonthIntervalType") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val schema = StructType(Seq(
        StructField("a", YearMonthIntervalType(YEAR)),
        StructField("b", YearMonthIntervalType(YEAR, MONTH)),
        StructField("c", YearMonthIntervalType(MONTH))
      ))
      val df = spark.createDataFrame(sparkContext.parallelize(Seq(
        Row(Period.ofMonths(13), Period.ofMonths(13), Period.ofMonths(13))
      )), schema)

      // Hive serde supports YearMonthIntervalType as input and output data type
      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          script = "cat",
          output = Seq(
            AttributeReference("a", YearMonthIntervalType(YEAR))(),
            AttributeReference("b", YearMonthIntervalType(YEAR, MONTH))(),
            AttributeReference("c", YearMonthIntervalType(MONTH))()),
          child = child,
          ioschema = hiveIOSchema),
        df.select($"a", $"b", $"c").collect())
    }
  }

  test("SPARK-34879: HiveInspectors throw overflow when" +
    " HiveIntervalDayTime overflow then DayTimeIntervalType") {
    withTempView("v") {
      val df = Seq(("579025220 15:30:06.000001000")).toDF("a")
      df.createTempView("v")

      val e = intercept[Exception] {
        checkAnswer(
          df,
          (child: SparkPlan) => createScriptTransformationExec(
            script = "cat",
            output = Seq(AttributeReference("a", DayTimeIntervalType())()),
            child = child,
            ioschema = hiveIOSchema),
          df.select($"a").collect())
      }.getMessage
      assert(e.contains("java.lang.ArithmeticException: long overflow"))
    }
  }
}
