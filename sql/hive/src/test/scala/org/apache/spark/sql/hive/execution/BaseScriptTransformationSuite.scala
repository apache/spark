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

import java.sql.{Date, Timestamp}
import java.util.Locale

import org.scalatest.Assertions._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, TaskContext, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class BaseScriptTransformationSuite extends SparkPlanTest with SQLTestUtils
  with TestHiveSingleton with BeforeAndAfterEach {

  def scriptType: String

  import spark.implicits._

  var noSerdeIOSchema: BaseScriptTransformIOSchema = _

  private var defaultUncaughtExceptionHandler: Thread.UncaughtExceptionHandler = _

  protected val uncaughtExceptionHandler = new TestUncaughtExceptionHandler

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    defaultUncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    Thread.setDefaultUncaughtExceptionHandler(defaultUncaughtExceptionHandler)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    uncaughtExceptionHandler.cleanStatus()
  }

  def createScriptTransformationExec(
      input: Seq[Expression],
      script: String,
      output: Seq[Attribute],
      child: SparkPlan,
      ioschema: BaseScriptTransformIOSchema): BaseScriptTransformationExec = {
    scriptType.toUpperCase(Locale.ROOT) match {
      case "SPARK" => new SparkScriptTransformationExec(
        input = input,
        script = script,
        output = output,
        child = child,
        ioschema = ioschema.asInstanceOf[SparkScriptIOSchema]
      )
      case "HIVE" => new HiveScriptTransformationExec(
        input = input,
        script = script,
        output = output,
        child = child,
        ioschema = ioschema.asInstanceOf[HiveScriptIOSchema]
      )
      case _ => throw new TestFailedException(
        "Test class implement from BaseScriptTransformationSuite" +
          " should override method `scriptType` to Spark or Hive", 0)
    }
  }

  test("cat without SerDe") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    checkAnswer(
      rowsDf,
      (child: SparkPlan) => createScriptTransformationExec(
        input = Seq(rowsDf.col("a").expr),
        script = "cat",
        output = Seq(AttributeReference("a", StringType)()),
        child = child,
        ioschema = noSerdeIOSchema
      ),
      rowsDf.collect())
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("script transformation should not swallow errors from upstream operators (no serde)") {
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
          ioschema = noSerdeIOSchema
        ),
        rowsDf.collect())
    }
    assert(e.getMessage().contains("intentional exception"))
    // Before SPARK-25158, uncaughtExceptionHandler will catch IllegalArgumentException
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-25990: TRANSFORM should handle different data types correctly") {
    assume(TestUtils.testCommandAvailable("python"))
    val scriptFilePath = getTestResourcePath("test_script.py")
    case class Struct(d: Int, str: String)
    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1),
          new Date(2020, 7, 1), new CalendarInterval(7, 1, 1000), Array(0, 1, 2),
          Map("a" -> 1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2),
          new Date(2020, 7, 2), new CalendarInterval(7, 2, 2000), Array(3, 4, 5),
          Map("b" -> 2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3),
          new Date(2020, 7, 3), new CalendarInterval(7, 3, 3000), Array(6, 7, 8),
          Map("c" -> 3))
      ).toDF("a", "b", "c", "d", "e", "f", "g", "h", "i")
          .select('a, 'b, 'c, 'd, 'e, 'f, 'g, 'h, 'i, struct('a, 'b).as("j"))
      // Note column d's data type is Decimal(38, 18)
      df.createTempView("v")

      assert(spark.table("v").schema ==
        StructType(Seq(StructField("a", IntegerType, false),
          StructField("b", StringType, true),
          StructField("c", DoubleType, false),
          StructField("d", DecimalType(38, 18), true),
          StructField("e", TimestampType, true),
          StructField("f", DateType, true),
          StructField("g", CalendarIntervalType, true),
          StructField("h", ArrayType(IntegerType, false), true),
          StructField("i", MapType(StringType, IntegerType, false), true),
          StructField("j", StructType(
            Seq(StructField("a", IntegerType, false),
              StructField("b", StringType, true))), false))))

      // Can't support convert script output data to ArrayType/MapType/StructType now,
      // return these column still as string
      val query = sql(
        s"""
           |SELECT
           |TRANSFORM(a, b, c, d, e, f, g, h, i, j)
           |USING 'python $scriptFilePath'
           |AS (
           |  a int,
           |  b string,
           |  c double,
           |  d decimal(1, 0),
           |  e timestamp,
           |  f date,
           |  g interval,
           |  h string,
           |  i string,
           |  j string)
           |FROM v
        """.stripMargin)

      checkAnswer(query, identity, df.select(
        'a, 'b, 'c, 'd, 'e, 'f, 'g,
        'h.cast("string"),
        'i.cast("string"),
        'j.cast("string")).collect())
    }
  }

  test("SPARK-30973: TRANSFORM should wait for the termination of the script (no serde)") {
    assume(TestUtils.testCommandAvailable("/bin/bash"))

    val rowsDf = Seq("a", "b", "c").map(Tuple1.apply).toDF("a")
    val e = intercept[SparkException] {
      val plan =
        createScriptTransformationExec(
          input = Seq(rowsDf.col("a").expr),
          script = "some_non_existent_command",
          output = Seq(AttributeReference("a", StringType)()),
          child = rowsDf.queryExecution.sparkPlan,
          ioschema = noSerdeIOSchema)
      SparkPlanTest.executePlan(plan, hiveContext)
    }
    assert(e.getMessage.contains("Subprocess exited with status"))
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }
}

case class ExceptionInjectingOperator(child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().map { x =>
      assert(TaskContext.get() != null) // Make sure that TaskContext is defined.
      Thread.sleep(1000) // This sleep gives the external process time to start.
      throw new IllegalArgumentException("intentional exception")
    }
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning
}
