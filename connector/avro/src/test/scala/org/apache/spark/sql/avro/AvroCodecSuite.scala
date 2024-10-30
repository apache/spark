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

package org.apache.spark.sql.avro

import java.util.Locale

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.FileSourceCodecSuite
import org.apache.spark.sql.internal.SQLConf

class AvroCodecSuite extends FileSourceCodecSuite {

  override def format: String = "avro"
  override val codecConfigName: String = SQLConf.AVRO_COMPRESSION_CODEC.key
  override protected def availableCodecs =
    AvroCompressionCodec.values().map(_.lowerCaseName()).iterator.to(Seq)

  (availableCodecs ++ availableCodecs.map(_.capitalize)).foreach { codec =>
    test(s"SPARK-46746: attach codec name to avro files - codec $codec") {
      withTable("avro_t") {
        sql(
          s"""CREATE TABLE avro_t
             |USING $format OPTIONS('compression'='$codec')
             |AS SELECT 1 as id""".stripMargin)
        spark
          .table("avro_t")
          .inputFiles.foreach { f =>
            assert(f.endsWith(s"$codec.avro".toLowerCase(Locale.ROOT).stripPrefix("uncompressed")))
          }
      }
    }
  }

  test("SPARK-46754: invalid compression codec name in avro table definition") {
    checkError(
      exception = intercept[SparkIllegalArgumentException](
        sql(
          s"""CREATE TABLE avro_t
             |USING $format OPTIONS('compression'='unsupported')
             |AS SELECT 1 as id""".stripMargin)),
      condition = "CODEC_SHORT_NAME_NOT_FOUND",
      sqlState = Some("42704"),
      parameters = Map("codecName" -> "unsupported")
    )
  }

  test("SPARK-46759: compression level support for zstandard codec") {
    Seq("9", "1").foreach { level =>
      withSQLConf(
        (SQLConf.AVRO_COMPRESSION_CODEC.key -> "zstandard"),
        (SQLConf.AVRO_ZSTANDARD_LEVEL.key -> level)) {
        withTable("avro_t") {
          sql(
            s"""CREATE TABLE avro_t
               |USING $format
               |AS SELECT 1 as id""".stripMargin)
          checkAnswer(spark.table("avro_t"), Seq(Row(1)))
        }
      }
    }
  }
}
