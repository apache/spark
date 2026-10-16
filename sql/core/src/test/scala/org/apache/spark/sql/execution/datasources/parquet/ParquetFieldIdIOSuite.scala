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

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class ParquetFieldIdIOSuite extends QueryTest with ParquetTest with SharedSparkSession  {

  private def withId(id: Int): Metadata =
    new MetadataBuilder().putLong(ParquetUtils.FIELD_ID_METADATA_KEY, id).build()

  test("Parquet reads infer fields using field ids correctly") {
    withTempDir { dir =>
      val readSchema =
        new StructType()
          .add("a", StringType, true, withId(0))
          .add("b", IntegerType, true, withId(1))

      val readSchemaMixed =
        new StructType()
          .add("name", StringType, true)
          .add("b", IntegerType, true, withId(1))

      val readSchemaMixedHalfMatched =
        new StructType()
          .add("unmatched", StringType, true)
          .add("b", IntegerType, true, withId(1))

      val writeSchema =
        new StructType()
          .add("random", IntegerType, true, withId(1))
          .add("name", StringType, true, withId(0))

      val readData = Seq(Row("text", 100), Row("more", 200))
      val readDataHalfMatched = Seq(Row(null, 100), Row(null, 200))
      val writeData = Seq(Row(100, "text"), Row(200, "more"))
      spark.createDataFrame(writeData.asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      withAllParquetReaders {
        // read with schema
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath), readData)
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath)
          .where("b < 50"), Seq.empty)
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath)
          .where("a >= 'oh'"), Row("text", 100) :: Nil)
        // read with mixed field-id/name schema
        checkAnswer(spark.read.schema(readSchemaMixed).parquet(dir.getCanonicalPath), readData)
        checkAnswer(spark.read.schema(readSchemaMixedHalfMatched)
          .parquet(dir.getCanonicalPath), readDataHalfMatched)

        // schema inference should pull into the schema with ids
        val reader = spark.read.parquet(dir.getCanonicalPath)
        assert(reader.schema == writeSchema)
        checkAnswer(reader.where("name >= 'oh'"), Row(100, "text") :: Nil)
      }
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

      withAllParquetReaders {
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath),
          // 3 different cases for the 3 columns to read:
          //   - a: ID 1 is not found, but there is column with name `a`, still return null
          //   - b: ID 2 is not found, return null
          //   - c: ID 3 is found, read it
          Row(null, null, 100) :: Row(null, null, 200) :: Nil)
      }
    }
  }

  test("SPARK-38094: absence of field ids: reading nested schema") {
    withTempDir { dir =>
      // now with nested schema/complex type
      val readSchema =
        new StructType()
          .add("a", IntegerType, true, withId(1))
          .add("b", ArrayType(StringType), true, withId(2))
          .add("c", new StructType().add("c1", IntegerType, true, withId(6)), true, withId(3))
          .add("d", MapType(StringType, StringType), true, withId(4))
          .add("e", IntegerType, true, withId(5))

      val writeSchema =
        new StructType()
          .add("a", IntegerType, true, withId(5))
          .add("randomName", StringType, true)

      val writeData = Seq(Row(100, "text"), Row(200, "more"))

      spark.createDataFrame(writeData.asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      withAllParquetReaders {
        checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath),
          // a, b, c, d all couldn't be found
          Row(null, null, null, null, 100) :: Row(null, null, null, null, 200) :: Nil)
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

      withAllParquetReaders {
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
      withAllParquetReaders {
        Seq(readSchema, readSchema.add("b", StringType, true)).foreach { schema =>
          val cause = intercept[SparkException] {
            spark.read.schema(schema).parquet(dir.getCanonicalPath).collect()
          }.getCause
          assert(cause.isInstanceOf[RuntimeException] &&
            cause.getMessage.contains("Parquet file schema doesn't contain any field Ids"))
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

  test("global read/write flag should work correctly") {
    withTempDir { dir =>
      val readSchema =
        new StructType()
          .add("some", IntegerType, true, withId(1))
          .add("other", StringType, true, withId(2))
          .add("name", StringType, true, withId(3))

      val writeSchema =
          new StructType()
            .add("a", IntegerType, true, withId(1))
            .add("rand1", StringType, true, withId(2))
            .add("rand2", StringType, true, withId(3))

      val writeData = Seq(Row(100, "text", "txt"), Row(200, "more", "mr"))

      val expectedResult = Seq(Row(null, null, null), Row(null, null, null))

      withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "false",
        SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true") {
        spark.createDataFrame(writeData.asJava, writeSchema)
          .write.mode("overwrite").parquet(dir.getCanonicalPath)
        withAllParquetReaders {
          // no field id found exception
          val cause = intercept[SparkException] {
            spark.read.schema(readSchema).parquet(dir.getCanonicalPath).collect()
          }.getCause
          assert(cause.isInstanceOf[RuntimeException] &&
            cause.getMessage.contains("Parquet file schema doesn't contain any field Ids"))
        }
      }

      withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "true",
        SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "false") {
        spark.createDataFrame(writeData.asJava, writeSchema)
          .write.mode("overwrite").parquet(dir.getCanonicalPath)
        withAllParquetReaders {
          // ids are there, but we don't use id for matching, so no results would be returned
          checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath), expectedResult)
        }
      }
    }
  }

  test("SPARK-52267: Field ID mapping when field name doesn't match") {
    withTempDir { dir =>
      val readSchema = new StructType().add("id1", LongType, true, withId(1))
      val writeSchema = new StructType().add("id2", IntegerType, true, withId(1))

      withSQLConf(SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED.key -> "true") {
        val writeData = Seq(Row(1), Row(2), Row(3))
        spark.createDataFrame(writeData.asJava, writeSchema)
          .write.mode("overwrite").parquet(dir.getCanonicalPath)
      }

      withAllParquetReaders {
        withSQLConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "false") {
          checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath),
            Seq(Row(null), Row(null), Row(null)))
        }
        // Without the fix, the result is unpredictable when PARQUET_FIELD_ID_READ_ENABLED is
        // enabled. It could cause NPE if OnHeapColumnVector is used in the scan. It could produce
        // incorrect results if OffHeapColumnVector is used.
        withSQLConf(SQLConf.PARQUET_FIELD_ID_READ_ENABLED.key -> "true") {
          checkAnswer(spark.read.schema(readSchema).parquet(dir.getCanonicalPath),
            Seq(Row(1L), Row(2L), Row(3L)))
        }
      }
    }
  }

  private def checkFieldRequestedMoreThanOnce(
      df: => DataFrame,
      requestedFields: Seq[String],
      parquetField: String): Unit = {
    // The reader raises the error on the executor, so it is the cause of the job failure.
    val cause = intercept[SparkException](df.collect()).getCause
    checkError(
      exception = cause.asInstanceOf[SparkRuntimeException],
      condition = "PARQUET_FIELD_REQUESTED_MORE_THAN_ONCE",
      parameters = Map(
        "requestedFields" -> requestedFields.map(f => s"`$f`").mkString(", "),
        "parquetField" -> s"`$parquetField`",
        "fieldIdReadConf" -> "\"spark.sql.parquet.fieldId.read.enabled\""))
  }

  test("SPARK-60262: a field id match and a name match must not select the same column") {
    withTempDir { dir =>
      // The file column `A` has field id 1. A requested field with id 1 selects it by id, and a
      // requested field without an id selects it by name.
      val writeSchema = new StructType().add("A", IntegerType, true, withId(1))
      spark.createDataFrame(Seq(Row(1), Row(2), Row(3)).asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      val byId = StructField("x", IntegerType, true, withId(1))
      Seq("false" -> "a", "true" -> "A").foreach { case (caseSensitive, nameOfA) =>
        withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
          val byName = StructField(nameOfA, IntegerType, true)
          withAllParquetReaders {
            Seq(Seq(byId, byName), Seq(byName, byId)).foreach { fields =>
              checkFieldRequestedMoreThanOnce(
                spark.read.schema(StructType(fields)).parquet(dir.getCanonicalPath),
                fields.map(_.name),
                "A")
            }
          }
        }
      }

      // A requested field that matches no column does not select the column either.
      withAllParquetReaders {
        checkAnswer(
          spark.read.schema(new StructType().add(byId).add("b", IntegerType, true))
            .parquet(dir.getCanonicalPath),
          Seq(Row(1, null), Row(2, null), Row(3, null)))
      }
    }
  }

  test("SPARK-60262: a field id match and a name match must not select the same nested column") {
    withTempDir { dir =>
      val writeSchema = new StructType().add(
        "s", new StructType().add("A", IntegerType, true, withId(2)), true, withId(1))
      spark.createDataFrame(Seq(Row(Row(1)), Row(Row(2)), Row(Row(3))).asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      val readSchema = new StructType().add(
        "s",
        new StructType()
          .add("x", IntegerType, true, withId(2))
          .add("a", IntegerType, true),
        true,
        withId(1))
      withAllParquetReaders {
        checkFieldRequestedMoreThanOnce(
          spark.read.schema(readSchema).parquet(dir.getCanonicalPath), Seq("x", "a"), "A")
      }
    }
  }

  test("SPARK-60262: two fields with the same field id must not select the same column") {
    withTempDir { dir =>
      val writeSchema = new StructType().add("A", IntegerType, true, withId(1))
      spark.createDataFrame(Seq(Row(1), Row(2), Row(3)).asJava, writeSchema)
        .write.mode("overwrite").parquet(dir.getCanonicalPath)

      val readSchema = new StructType()
        .add("x", IntegerType, true, withId(1))
        .add("y", IntegerType, true, withId(1))
      withAllParquetReaders {
        checkFieldRequestedMoreThanOnce(
          spark.read.schema(readSchema).parquet(dir.getCanonicalPath), Seq("x", "y"), "A")
      }
    }
  }
}
