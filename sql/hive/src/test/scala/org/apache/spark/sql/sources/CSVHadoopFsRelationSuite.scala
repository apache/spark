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

package org.apache.spark.sql.sources

import org.apache.hadoop.fs.Path

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.types._

class CSVHadoopFsRelationSuite extends HadoopFsRelationTest {
  override val dataSourceName: String = "csv"

  override val extraReadOptions: Map[String, String] =
    Map("header" -> "true", "inferSchema" -> "true")

  override val extraWriteOptions: Map[String, String] = Map("header" -> "true")

  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => false
    // `StringType` test is too flaky. Seems random generate data affects delimiter
    // for writing and CSV parse does not recognize this.
    case _: StringType => false
    case _: BinaryType => false
    case _: CalendarIntervalType => false
    case _: ArrayType => false
    case _: MapType => false
    case _: StructType => false
    // Currently, this writes `DateType` and `TimestampType` as a long value.
    // Since `dateFormat` is not yet supported for writing, this is disabled for now.
    case _: DateType => false
    case _: TimestampType => false
    case _: UserDefinedType[_] => false
    case _ => true
  }

  test("save()/load() - partitioned table - simple queries - partition columns in data") {
    withTempDir { file =>
      val basePath = new Path(file.getCanonicalPath)
      val fs = basePath.getFileSystem(SparkHadoopUtil.get.conf)
      val qualifiedBasePath = fs.makeQualified(basePath)

      for (p1 <- 1 to 2; p2 <- Seq("foo", "bar")) {
        val partitionDir = new Path(qualifiedBasePath, s"p1=$p1/p2=$p2")
        val header = Seq("a,b")
        val data = (1 to 3).map(i => s"""$i,val_$i""")
        sparkContext
          .parallelize(header ++ data)
          .saveAsTextFile(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        hiveContext.read.format(dataSourceName)
          .option("dataSchema", dataSchemaWithPartition.json)
          .option("inferSchema", "true")
          .option("header", "true")
          .load(file.getCanonicalPath))
    }
  }
}
