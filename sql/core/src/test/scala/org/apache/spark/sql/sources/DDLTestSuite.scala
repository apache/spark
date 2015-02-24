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

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class DDLScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleDDLScan(parameters("from").toInt, parameters("TO").toInt)(sqlContext)
  }
}

case class SimpleDDLScan(from: Int, to: Int)(@transient val sqlContext: SQLContext)
  extends TableScan {

  override def schema =
    StructType(Seq(
      StructField("intType", IntegerType, nullable = false,
        new MetadataBuilder().putString("comment", "test comment").build()),
      StructField("stringType", StringType, nullable = false),
      StructField("dateType", DateType, nullable = false),
      StructField("timestampType", TimestampType, nullable = false),
      StructField("doubleType", DoubleType, nullable = false),
      StructField("bigintType", LongType, nullable = false),
      StructField("tinyintType", ByteType, nullable = false),
      StructField("decimalType", DecimalType.Unlimited, nullable = false),
      StructField("fixedDecimalType", DecimalType(5,1), nullable = false),
      StructField("binaryType", BinaryType, nullable = false),
      StructField("booleanType", BooleanType, nullable = false),
      StructField("smallIntType", ShortType, nullable = false),
      StructField("floatType", FloatType, nullable = false),
      StructField("mapType", MapType(StringType, StringType)),
      StructField("arrayType", ArrayType(StringType)),
      StructField("structType",
        StructType(StructField("f1",StringType) ::
          (StructField("f2",IntegerType)) :: Nil
        )
      )
    ))


  override def buildScan() = sqlContext.sparkContext.parallelize(from to to).
    map(e => Row(s"people$e", e * 2))
}

class DDLTestSuite extends DataSourceTest {
  import caseInsensisitiveContext._

  before {
      sql(
          """
          |CREATE TEMPORARY TABLE ddlPeople
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10'
          |)
          """.stripMargin)
  }

  sqlTest(
      "describe ddlPeople",
      Seq(
        Row("intType", "int", "test comment"),
        Row("stringType", "string", ""),
        Row("dateType", "date", ""),
        Row("timestampType", "timestamp", ""),
        Row("doubleType", "double", ""),
        Row("bigintType", "bigint", ""),
        Row("tinyintType", "tinyint", ""),
        Row("decimalType", "decimal(10,0)", ""),
        Row("fixedDecimalType", "decimal(5,1)", ""),
        Row("binaryType", "binary", ""),
        Row("booleanType", "boolean", ""),
        Row("smallIntType", "smallint", ""),
        Row("floatType", "float", ""),
        Row("mapType", "map<string,string>", ""),
        Row("arrayType", "array<string>", ""),
        Row("structType", "struct<f1:string,f2:int>", "")
      ))
}
