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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

private[sql] abstract class DataSourceTest extends QueryTest {

  protected def sqlTest(sqlString: String, expectedAnswer: Seq[Row],
      enableRegex: Boolean = false): Unit = {
    test(sqlString) {
      withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> enableRegex.toString) {
        checkAnswer(spark.sql(sqlString), expectedAnswer)
      }
    }
  }

}

class DDLScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleDDLScan(
      parameters("from").toInt,
      parameters("TO").toInt,
      parameters("Table"))(sqlContext.sparkSession)
  }
}

case class SimpleDDLScan(
    from: Int,
    to: Int,
    table: String)(@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType =
    StructType(Seq(
      StructField("intType", IntegerType, nullable = false).withComment(s"test comment $table"),
      StructField("stringType", StringType, nullable = false),
      StructField("dateType", DateType, nullable = false),
      StructField("timestampType", TimestampType, nullable = false),
      StructField("doubleType", DoubleType, nullable = false),
      StructField("bigintType", LongType, nullable = false),
      StructField("tinyintType", ByteType, nullable = false),
      StructField("decimalType", DecimalType.USER_DEFAULT, nullable = false),
      StructField("fixedDecimalType", DecimalType(5, 1), nullable = false),
      StructField("binaryType", BinaryType, nullable = false),
      StructField("booleanType", BooleanType, nullable = false),
      StructField("smallIntType", ShortType, nullable = false),
      StructField("floatType", FloatType, nullable = false),
      StructField("mapType", MapType(StringType, StringType)),
      StructField("arrayType", ArrayType(StringType)),
      StructField("structType",
        StructType(StructField("f1", StringType) :: StructField("f2", IntegerType) :: Nil
        )
      )
    ))

  override def needConversion: Boolean = false

  override def buildScan(): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    sparkSession.sparkContext.parallelize(from to to).map { e =>
      InternalRow(UTF8String.fromString(s"people$e"), e * 2)
    }.asInstanceOf[RDD[Row]]
  }
}
