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

import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, TestUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class HiveScriptTransformationSuite extends BaseScriptTransformationSuite with TestHiveSingleton {
  import testImplicits._

  import ScriptTransformationIOSchema._

  override def createScriptTransformationExec(
      input: Seq[Expression],
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec = {
    HiveScriptTransformationExec(
      input = input,
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
        input = Seq(rowsDf.col("a").expr),
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
          input = Seq(rowsDf.col("a").expr),
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
          input = Seq(rowsDf.col("a").expr),
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
        input = Seq(rowsDf.col("name").expr),
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
          input = Seq(rowsDf.col("a").expr),
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
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("c").expr,
            df.col("d").expr,
            df.col("e").expr),
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

  test("SPARK-32400: TRANSFORM doesn't support CalenderIntervalType/UserDefinedType (hive serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))
    withTempView("v") {
      val df = Seq(
        (1, new CalendarInterval(7, 1, 1000), new TestUDT.MyDenseVector(Array(1, 2, 3))),
        (1, new CalendarInterval(7, 1, 1000), new TestUDT.MyDenseVector(Array(1, 2, 3)))
      ).toDF("a", "b", "c")
      df.createTempView("v")

      val e1 = intercept[SparkException] {
        val plan = createScriptTransformationExec(
          input = Seq(df.col("a").expr, df.col("b").expr),
          script = "cat",
          output = Seq(
            AttributeReference("a", IntegerType)(),
            AttributeReference("b", CalendarIntervalType)()),
          child = df.queryExecution.sparkPlan,
          ioschema = hiveIOSchema)
        SparkPlanTest.executePlan(plan, hiveContext)
      }.getMessage
      assert(e1.contains("interval cannot be converted to Hive TypeInfo"))

      val e2 = intercept[SparkException] {
        val plan = createScriptTransformationExec(
          input = Seq(df.col("a").expr, df.col("c").expr),
          script = "cat",
          output = Seq(
            AttributeReference("a", IntegerType)(),
            AttributeReference("c", new TestUDT.MyDenseVectorUDT)()),
          child = df.queryExecution.sparkPlan,
          ioschema = hiveIOSchema)
        SparkPlanTest.executePlan(plan, hiveContext)
      }.getMessage
      assert(e2.contains("array<double> cannot be converted to Hive TypeInfo"))
    }
  }

  test("SPARK-32400: TRANSFORM doesn't support" +
    " CalenderIntervalType/UserDefinedType end to end (hive serde)") {
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

  test("SPARK-38075: ORDER BY with LIMIT should not add fake rows") {
    withTempView("v") {
      val df = Seq((1), (2), (3)).toDF("a")
      df.createTempView("v")
      checkAnswer(sql(
        """
          |SELECT TRANSFORM(a)
          |  USING 'cat' AS (a)
          |FROM v
          |ORDER BY a
          |LIMIT 10
          |""".stripMargin),
        identity,
        Row("1") :: Row("2") :: Row("3") :: Nil)
    }
  }
}
