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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.execution.PhysicalRDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class SimpleTextHadoopFsRelationSuite extends HadoopFsRelationTest {
  import testImplicits._

  override val dataSourceName: String = classOf[SimpleTextSource].getCanonicalName

  // We have a very limited number of supported types at here since it is just for a
  // test relation and we do very basic testing at here.
  override protected def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: BinaryType => false
    // We are using random data generator and the generated strings are not really valid string.
    case _: StringType => false
    case _: BooleanType => false // see https://issues.apache.org/jira/browse/SPARK-10442
    case _: CalendarIntervalType => false
    case _: DateType => false
    case _: TimestampType => false
    case _: ArrayType => false
    case _: MapType => false
    case _: StructType => false
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
        sparkContext
          .parallelize(for (i <- 1 to 3) yield s"$i,val_$i,$p1")
          .saveAsTextFile(partitionDir.toString)
      }

      val dataSchemaWithPartition =
        StructType(dataSchema.fields :+ StructField("p1", IntegerType, nullable = true))

      checkQueries(
        hiveContext.read.format(dataSourceName)
          .option("dataSchema", dataSchemaWithPartition.json)
          .load(file.getCanonicalPath))
    }
  }

  private val writer = testDF.write.option("dataSchema", dataSchema.json).format(dataSourceName)
  private val reader = sqlContext.read.option("dataSchema", dataSchema.json).format(dataSourceName)

  test("unhandledFilters") {
    withTempPath { dir =>

      val path = dir.getCanonicalPath
      writer.save(s"$path/p=0")
      writer.save(s"$path/p=1")

      val isOdd = udf((_: Int) % 2 == 1)
      val df = reader.load(path)
        .filter(
          // This filter is inconvertible
          isOdd('a) &&
            // This filter is convertible but unhandled
            'a > 1 &&
            // This filter is convertible and handled
            'b > "val_1" &&
            // This filter references a partiiton column, won't be pushed down
            'p === 1
        ).select('a, 'p)
      val rawScan = df.queryExecution.executedPlan collect {
        case p: PhysicalRDD => p
      } match {
        case Seq(p) => p
      }

      val outputSchema = new StructType().add("a", IntegerType).add("p", IntegerType)

      assertResult(Set((2, 1), (3, 1))) {
        rawScan.execute().collect()
          .map { CatalystTypeConverters.convertToScala(_, outputSchema) }
          .map { case Row(a, p) => (a, p) }.toSet
      }

      checkAnswer(df, Row(3, 1))
    }
  }
}
