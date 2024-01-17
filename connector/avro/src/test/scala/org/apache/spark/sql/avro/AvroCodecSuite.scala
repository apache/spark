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

import org.apache.spark.sql.execution.datasources.FileSourceCodecSuite
import org.apache.spark.sql.internal.SQLConf

class AvroCodecSuite extends FileSourceCodecSuite {

  override def format: String = "avro"
  override val codecConfigName: String = SQLConf.AVRO_COMPRESSION_CODEC.key
  override protected def availableCodecs =
    AvroCompressionCodec.values().map(_.lowerCaseName()).iterator.to(Seq)

  availableCodecs.foreach { codec =>
    test(s"SPARK-46746: attach codec name to avro files - codec $codec") {
      withTable("avro_t") {
        sql(
          s"""CREATE TABLE avro_t
             | USING $format OPTIONS('compression'='$codec')
             | AS SELECT 1 as id
             | """.stripMargin)
        spark.table("avro_t")
          .inputFiles.foreach { file =>
            assert(file.endsWith(s"$codec.avro".stripPrefix("uncompressed")))
          }
      }
    }
  }
}
