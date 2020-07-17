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

import org.apache.spark.TestUtils
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.{ScriptTransformationIOSchema, SparkPlan}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval

class SparkScriptTransformationSuite extends BaseScriptTransformationSuite {

  import spark.implicits._

  override def scriptType: String = "SPARK"

  noSerdeIOSchema = ScriptTransformationIOSchema(
    inputRowFormat = Seq.empty,
    outputRowFormat = Seq.empty,
    inputSerdeClass = None,
    outputSerdeClass = None,
    inputSerdeProps = Seq.empty,
    outputSerdeProps = Seq.empty,
    recordReaderClass = None,
    recordWriterClass = None,
    schemaLess = false
  )

  test("SPARK-32106: SparkScriptTransformExec should handle different data types correctly") {
    assume(TestUtils.testCommandAvailable("python"))
    val scriptFilePath = getTestResourcePath("test_spark_script.py")
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
            AttributeReference("c", DoubleType)(),
            AttributeReference("d", DecimalType(1, 0))(),
            AttributeReference("e", TimestampType)(),
            AttributeReference("f", DateType)(),
            AttributeReference("g", CalendarIntervalType)(),
            AttributeReference("h", StringType)(),
            AttributeReference("i", StringType)(),
            AttributeReference("j", StringType)()
          ),
          child = child,
          ioschema = noSerdeIOSchema
        ),
        df.select(
          'a, 'b, 'c, 'd, 'e, 'f, 'g,
          'h.cast("string"),
          'i.cast("string"),
          'j.cast("string")).collect())
    }
  }
}
