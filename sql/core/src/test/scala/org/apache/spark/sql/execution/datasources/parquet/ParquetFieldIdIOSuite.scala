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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, Metadata, MetadataBuilder, StringType, StructType}

class ParquetFieldIdIOSuite extends QueryTest with ParquetTest with SharedSparkSession  {

  private def withId(id: Int): Metadata =
    new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()

  /**
   * Field id is supported in OSS vectorized reader at the moment.
   * parquet-mr support is coming soon.
   */
  private def withAllSupportedReaders(code: => Unit): Unit = {
   withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true")(code)
  }

  test("general test") {
    withTempDir { dir =>
      val readSchema =
        new StructType().add(
          "a", StringType, true, withId(0))
          .add("b", IntegerType, true, withId(1))

      val writeSchema =
        new StructType()
          .add("random", IntegerType, true, withId(1))
          .add("name", StringType, true, withId(0))

      val readData = Seq(Row("text", 100), Row("more", 200))
      val writeData = Seq(Row(100, "text"), Row(200, "more"))
      spark.createDataFrame(writeData.asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      withAllSupportedReaders {
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath), readData)
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath)
          .where("b < 50"), Seq.empty)
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath)
          .where("a >= 'oh'"), Row("text", 100) :: Nil)
      }

      // blocked for Parquet-mr reader
      val e = intercept[SparkException] {
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
          checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath), readData)
        }
      }
      val cause = e.getCause
      assert(cause.isInstanceOf[java.io.IOException] &&
        cause.getMessage.contains("Parquet-mr reader does not support schema with field IDs."))
    }
  }

  test("absence of field ids") {
    withTempDir { dir =>
      val readSchema =
        new StructType()
          .add("a", IntegerType, true, withId(1))
          .add("b", StringType, true, withId(2))
          .add("c", IntegerType, true, withId(3))

      val writeSchema =
        new StructType()
          .add("a", IntegerType, true, withId(3))
          .add("randomName", StringType, true)

      val writeData = Seq(Row(100, "text"), Row(200, "more"))

      spark.createDataFrame(writeData.asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      withAllSupportedReaders {
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath),
          // 3 different cases for the 3 columns to read:
          //   - a: ID 1 is not found, but there is column with name `a`, still return null
          //   - b: ID 2 is not found, return null
          //   - c: ID 3 is found, read it
          Row(null, null, 100) :: Row(null, null, 200) :: Nil)
      }
    }
  }

  test("multiple id matches") {
    withTempDir { dir =>
      val readSchema =
        new StructType()
          .add("a", IntegerType, true, withId(1))

      val writeSchema =
        new StructType()
          .add("a", IntegerType, true, withId(1))
          .add("rand1", StringType, true, withId(2))
          .add("rand2", StringType, true, withId(1))

      val writeData = Seq(Row(100, "text", "txt"), Row(200, "more", "mr"))

      spark.createDataFrame(writeData.asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      withAllSupportedReaders {
        val cause = intercept[SparkException] {
          spark.read.schema(readSchema).parquet(dir.getCanonicalPath).collect()
        }.getCause
        assert(cause.isInstanceOf[RuntimeException] &&
          cause.getMessage.contains("Found duplicate field(s)"))
      }
    }
  }

  test("read parquet file without ids") {
    withTempDir { dir =>
      val readSchema =
        new StructType()
          .add("a", IntegerType, true, withId(1))

      val writeSchema =
        new StructType()
          .add("a", IntegerType, true)
          .add("rand1", StringType, true)
          .add("rand2", StringType, true)

      val writeData = Seq(Row(100, "text", "txt"), Row(200, "more", "mr"))
      spark.createDataFrame(writeData.asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)
      withAllSupportedReaders {
        Seq(readSchema, readSchema.add("b", StringType, true)).foreach { schema =>
          val cause = intercept[SparkException] {
            spark.read.schema(schema).parquet(dir.getCanonicalPath).collect()
          }.getCause
          assert(cause.isInstanceOf[RuntimeException] &&
            cause.getMessage.contains("Parquet file schema doesn't contain field Ids"))
          val expectedValues = (1 to schema.length).map(_ => null)
          withSQLConf(SQLConf.IGNORE_MISSING_PARQUET_FIELD_ID.key -> "true") {
            checkAnswer(
              spark.read.schema(schema).parquet(dir.getCanonicalPath),
              Row(expectedValues: _*) :: Row(expectedValues: _*) :: Nil)
          }
        }
      }
    }
  }
}
