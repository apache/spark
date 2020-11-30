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

package org.apache.spark.sql.execution

import java.sql.{Date, Timestamp}

import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.scalatest.Assertions._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.exceptions.TestFailedException

import org.apache.spark.{SparkException, TaskContext, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

abstract class BaseScriptTransformationSuite extends SparkPlanTest with SQLTestUtils
  with BeforeAndAfterEach {
  import testImplicits._
  import ScriptTransformationIOSchema._

  protected val uncaughtExceptionHandler = new TestUncaughtExceptionHandler

  private var defaultUncaughtExceptionHandler: Thread.UncaughtExceptionHandler = _

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
      ioschema: ScriptTransformationIOSchema): BaseScriptTransformationExec

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
        ioschema = defaultIOSchema
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
          ioschema = defaultIOSchema
        ),
        rowsDf.collect())
    }
    assert(e.getMessage().contains("intentional exception"))
    // Before SPARK-25158, uncaughtExceptionHandler will catch IllegalArgumentException
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  test("SPARK-25990: TRANSFORM should handle different data types correctly") {
    assume(TestUtils.testCommandAvailable("python"))
    val scriptFilePath = copyAndGetResourceFile("test_script.py", ".py").getAbsoluteFile

    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
      ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)
      df.createTempView("v")

      val query = sql(
        s"""
           |SELECT
           |TRANSFORM(a, b, c, d, e)
           |USING 'python $scriptFilePath' AS (a, b, c, d, e)
           |FROM v
        """.stripMargin)

      checkAnswer(query, identity, df.select(
        'a.cast("string"),
        'b.cast("string"),
        'c.cast("string"),
        'd.cast("string"),
        'e.cast("string")).collect())
    }
  }

  test("SPARK-32388: TRANSFORM should handle schema less correctly (no serde)") {
    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
      ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)

      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("a").expr,
            df.col("b").expr,
            df.col("c").expr,
            df.col("d").expr,
            df.col("e").expr),
          script = "cat",
          output = Seq(
            AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()),
          child = child,
          ioschema = defaultIOSchema.copy(schemaLess = true)
        ),
        df.select(
          'a.cast("string").as("key"),
          'b.cast("string").as("value")).collect())

      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("a").expr,
            df.col("b").expr),
          script = "cat",
          output = Seq(
            AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()),
          child = child,
          ioschema = defaultIOSchema.copy(schemaLess = true)
        ),
        df.select(
          'a.cast("string").as("key"),
          'b.cast("string").as("value")).collect())

      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("a").expr),
          script = "cat",
          output = Seq(
            AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()),
          child = child,
          ioschema = defaultIOSchema.copy(schemaLess = true)
        ),
        df.select(
          'a.cast("string").as("key"),
          lit(null)).collect())
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
          ioschema = defaultIOSchema)
      SparkPlanTest.executePlan(plan, spark.sqlContext)
    }
    assert(e.getMessage.contains("Subprocess exited with status"))
    assert(uncaughtExceptionHandler.exception.isEmpty)
  }

  def testBasicInputDataTypesWith(serde: ScriptTransformationIOSchema, testName: String): Unit = {
    test(s"SPARK-32400: TRANSFORM should support basic data types as input ($testName)") {
      assume(TestUtils.testCommandAvailable("python"))
      withTempView("v") {
        val df = Seq(
          (1, "1", 1.0f, 1.0, 11.toByte, BigDecimal(1.0), new Timestamp(1),
            new Date(2020, 7, 1), true),
          (2, "2", 2.0f, 2.0, 22.toByte, BigDecimal(2.0), new Timestamp(2),
            new Date(2020, 7, 2), true),
          (3, "3", 3.0f, 3.0, 33.toByte, BigDecimal(3.0), new Timestamp(3),
            new Date(2020, 7, 3), false)
        ).toDF("a", "b", "c", "d", "e", "f", "g", "h", "i")
          .withColumn("j", lit("abc").cast("binary"))

        checkAnswer(
          df,
          (child: SparkPlan) => createScriptTransformationExec(
            input = Seq(
              df.col("a").expr,
              df.col("b").expr,
              df.col("c").expr,
              df.col("d").expr,
              df.col("e").expr,
              df.col("f").expr,
              df.col("g").expr,
              df.col("h").expr,
              df.col("i").expr,
              df.col("j").expr),
            script = "cat",
            output = Seq(
              AttributeReference("a", IntegerType)(),
              AttributeReference("b", StringType)(),
              AttributeReference("c", FloatType)(),
              AttributeReference("d", DoubleType)(),
              AttributeReference("e", ByteType)(),
              AttributeReference("f", DecimalType(38, 18))(),
              AttributeReference("g", TimestampType)(),
              AttributeReference("h", DateType)(),
              AttributeReference("i", BooleanType)(),
              AttributeReference("j", BinaryType)()),
            child = child,
            ioschema = serde
          ),
          df.select('a, 'b, 'c, 'd, 'e, 'f, 'g, 'h, 'i, 'j).collect())
      }
    }
  }

  testBasicInputDataTypesWith(defaultIOSchema, "no serde")

  test("SPARK-32400: TRANSFORM should support more data types (interval, array, map, struct " +
    "and udt) as input (no serde)") {
    assume(TestUtils.testCommandAvailable("python"))
    withTempView("v") {
      val df = Seq(
        (new CalendarInterval(7, 1, 1000), Array(0, 1, 2), Map("a" -> 1), (1, 2),
          new SimpleTuple(1, 1L)),
        (new CalendarInterval(7, 2, 2000), Array(3, 4, 5), Map("b" -> 2), (3, 4),
          new SimpleTuple(1, 1L)),
        (new CalendarInterval(7, 3, 3000), Array(6, 7, 8), Map("c" -> 3), (5, 6),
          new SimpleTuple(1, 1L))
      ).toDF("a", "b", "c", "d", "e")

      // Can't support convert script output data to ArrayType/MapType/StructType now,
      // return these column still as string.
      // For UserDefinedType, if user defined deserialize method to support convert string
      // to UserType like [[SimpleTupleUDT]], we can support convert to this UDT, else we
      // will return null value as column.
      checkAnswer(
        df,
        (child: SparkPlan) => createScriptTransformationExec(
          input = Seq(
            df.col("a").expr,
            df.col("b").expr,
            df.col("c").expr,
            df.col("d").expr,
            df.col("e").expr),
          script = "cat",
          output = Seq(
            AttributeReference("a", CalendarIntervalType)(),
            AttributeReference("b", StringType)(),
            AttributeReference("c", StringType)(),
            AttributeReference("d", StringType)(),
            AttributeReference("e", new SimpleTupleUDT)()),
          child = child,
          ioschema = defaultIOSchema
        ),
        df.select('a, 'b.cast("string"), 'c.cast("string"), 'd.cast("string"), 'e).collect())
    }
  }

  test("SPARK-32400: TRANSFORM should respect DATETIME_JAVA8API_ENABLED (no serde)") {
    assume(TestUtils.testCommandAvailable("python"))
    Array(false, true).foreach { java8AapiEnable =>
      withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8AapiEnable.toString) {
        withTempView("v") {
          val df = Seq(
            (new Timestamp(1), new Date(2020, 7, 1)),
            (new Timestamp(2), new Date(2020, 7, 2)),
            (new Timestamp(3), new Date(2020, 7, 3))
          ).toDF("a", "b")
          df.createTempView("v")

          val query = sql(
            """
              |SELECT TRANSFORM (a, b)
              |USING 'cat' AS (a timestamp, b date)
              |FROM v
            """.stripMargin)
          checkAnswer(query, identity, df.select('a, 'b).collect())
        }
      }
    }
  }

  test("SPARK-32608: Script Transform ROW FORMAT DELIMIT value should format value") {
    withTempView("v") {
      val df = Seq(
        (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
        (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
        (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
      ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)
      df.createTempView("v")

      // input/output with same delimit
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  ROW FORMAT DELIMITED
             |  FIELDS TERMINATED BY ','
             |  COLLECTION ITEMS TERMINATED BY '#'
             |  MAP KEYS TERMINATED BY '@'
             |  LINES TERMINATED BY '\n'
             |  NULL DEFINED AS 'null'
             |  USING 'cat' AS (a, b, c, d, e)
             |  ROW FORMAT DELIMITED
             |  FIELDS TERMINATED BY ','
             |  COLLECTION ITEMS TERMINATED BY '#'
             |  MAP KEYS TERMINATED BY '@'
             |  LINES TERMINATED BY '\n'
             |  NULL DEFINED AS 'NULL'
             |FROM v
        """.stripMargin), identity, df.select(
          'a.cast("string"),
          'b.cast("string"),
          'c.cast("string"),
          'd.cast("string"),
          'e.cast("string")).collect())

      // input/output with different delimit and show result
      checkAnswer(
        sql(
          s"""
             |SELECT TRANSFORM(a, b, c, d, e)
             |  ROW FORMAT DELIMITED
             |  FIELDS TERMINATED BY ','
             |  LINES TERMINATED BY '\n'
             |  NULL DEFINED AS 'null'
             |  USING 'cat' AS (value)
             |  ROW FORMAT DELIMITED
             |  FIELDS TERMINATED BY '&'
             |  LINES TERMINATED BY '\n'
             |  NULL DEFINED AS 'NULL'
             |FROM v
        """.stripMargin), identity, df.select(
          concat_ws(",",
            'a.cast("string"),
            'b.cast("string"),
            'c.cast("string"),
            'd.cast("string"),
            'e.cast("string"))).collect())
    }
  }

  test("SPARK-32667: SCRIPT TRANSFORM pad null value to fill column" +
    " when without schema less (no-serde)") {
    val df = Seq(
      (1, "1", 1.0, BigDecimal(1.0), new Timestamp(1)),
      (2, "2", 2.0, BigDecimal(2.0), new Timestamp(2)),
      (3, "3", 3.0, BigDecimal(3.0), new Timestamp(3))
    ).toDF("a", "b", "c", "d", "e") // Note column d's data type is Decimal(38, 18)

    checkAnswer(
      df,
      (child: SparkPlan) => createScriptTransformationExec(
        input = Seq(
          df.col("a").expr,
          df.col("b").expr),
        script = "cat",
        output = Seq(
          AttributeReference("a", StringType)(),
          AttributeReference("b", StringType)(),
          AttributeReference("c", StringType)(),
          AttributeReference("d", StringType)()),
        child = child,
        ioschema = defaultIOSchema
      ),
      df.select(
        'a.cast("string").as("a"),
        'b.cast("string").as("b"),
        lit(null), lit(null)).collect())
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

@SQLUserDefinedType(udt = classOf[SimpleTupleUDT])
private class SimpleTuple(val id: Int, val size: Long) extends Serializable {

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other match {
    case v: SimpleTuple => this.id == v.id && this.size == v.size
    case _ => false
  }

  override def toString: String =
    compact(render(
      ("id" -> id) ~
        ("size" -> size)
    ))
}

private class SimpleTupleUDT extends UserDefinedType[SimpleTuple] {

  override def sqlType: DataType = StructType(
    StructField("id", IntegerType, false) ::
      StructField("size", LongType, false) ::
      Nil)

  override def serialize(sql: SimpleTuple): Any = {
    val row = new GenericInternalRow(2)
    row.setInt(0, sql.id)
    row.setLong(1, sql.size)
    row
  }

  override def deserialize(datum: Any): SimpleTuple = {
    datum match {
      case str: String =>
        implicit val format = DefaultFormats
        val json = parse(str)
        new SimpleTuple((json \ "id").extract[Int], (json \ "size").extract[Long])
      case data: InternalRow if data.numFields == 2 =>
        new SimpleTuple(data.getInt(0), data.getLong(1))
      case _ => null
    }
  }

  override def userClass: Class[SimpleTuple] = classOf[SimpleTuple]

  override def asNullable: SimpleTupleUDT = this

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[SimpleTupleUDT]
  }
}
