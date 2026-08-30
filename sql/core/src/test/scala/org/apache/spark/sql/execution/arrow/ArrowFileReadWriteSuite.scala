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
package org.apache.spark.sql.execution.arrow

import java.io.File
import java.nio.channels.Channels
import java.nio.file.Files

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.{FieldVector, VectorSchemaRoot, ViewVarCharVector}
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

class ArrowFileReadWriteSuite extends SharedSparkSession {

  private var tempDataPath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDataPath = Utils.createTempDir(namePrefix = "arrowFileReadWrite").getAbsolutePath
  }

  test("simple") {
    val df = spark.range(0, 100, 1, 10).select(
      col("id"),
      lit(1).alias("int"),
      lit(2L).alias("long"),
      lit(3.0).alias("double"),
      lit("a string").alias("str"),
      lit(Array(1.0, 2.0, Double.NaN, Double.NegativeInfinity)).alias("arr"),
      col("id").cast("timestamp").alias("ts"))

    val path = new File(tempDataPath, "simple.arrowfile").toPath
    ArrowFileReadWrite.save(df, path)

    val df2 = ArrowFileReadWrite.load(spark, path)
    checkAnswer(df, df2)
  }

  test("empty dataframe") {
    val df = spark.range(0).withColumn("v", lit(1))
    assert(df.count() === 0)

    val path = new File(tempDataPath, "empty.arrowfile").toPath
    ArrowFileReadWrite.save(df, path)

    val df2 = ArrowFileReadWrite.load(spark, path)
    checkAnswer(df, df2)
  }

  test("loading a file whose layout differs from the canonical Arrow encoding fails fast") {
    // `load` rebuilds vectors from the Spark schema, so an Arrow type that converts to a Spark
    // type but is encoded differently (here Utf8View vs Utf8) cannot be loaded; it must be
    // rejected at the schema check instead of having its buffers misread as garbage values.
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("stringViewFile", 0, Long.MaxValue)
    val vector = new ViewVarCharVector("v", allocator)
    vector.allocateNew()
    val bytes = "a-string-longer-than-twelve-bytes".getBytes("utf8")
    vector.setSafe(0, bytes, 0, bytes.length)
    vector.setValueCount(1)
    val root = new VectorSchemaRoot(Seq[FieldVector](vector).asJava)
    val path = new File(tempDataPath, "stringview.arrowfile").toPath
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(Files.newOutputStream(path)))
    writer.start()
    writer.writeBatch()
    writer.close()
    root.close()
    allocator.close()

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        ArrowFileReadWrite.load(spark, path)
      },
      condition = "UNSUPPORTED_ARROWTYPE",
      parameters = Map("typeName" -> ArrowType.Utf8View.INSTANCE.toString))
  }
}
