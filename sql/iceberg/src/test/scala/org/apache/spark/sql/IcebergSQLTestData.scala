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

package org.apache.spark.sql

import org.apache.spark.sql.test.SQLTestData

trait IcebergSQLTestData extends SQLTestData {

  override def loadTestData(): Unit = {
    super.loadTestData()
    // delete the created temp views and instead create iceberg tables
    spark.sql("drop view if exists emptyTestData")
    emptyTestData.writeTo("emptyTestData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()

    spark.sql("drop view if exists testData")
    testData.writeTo("testData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()

    spark.sql("drop view if exists testData2")
    testData2.writeTo("testData2").tableProperty("write.format.default", "parquet").
      using("iceberg").create()

    spark.sql("drop view if exists testData3")
    testData3.writeTo("testData3").tableProperty("write.format.default", "parquet").
      using("iceberg").create()

    spark.sql("drop view if exists negativeData")
    negativeData.writeTo("negativeData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()

    spark.sql("drop view if exists largeAndSmallInts")
    largeAndSmallInts.writeTo("largeAndSmallInts").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists decimalData")
    decimalData.writeTo("decimalData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists binaryData")
    binaryData.writeTo("binaryData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    
    spark.sql("drop view if exists upperCaseData")
    upperCaseData.writeTo("upperCaseData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists lowerCaseData")
    lowerCaseData.writeTo("lowerCaseData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    
    spark.sql("drop view if exists nullInts")
    nullInts.writeTo("nullInts").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists allNulls")
    allNulls.writeTo("allNulls").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists nullStrings")
    nullStrings.writeTo("nullStrings").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists tableName")
    tableName.writeTo("tableName").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists person")
    person.writeTo("person").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists salary")
    salary.writeTo("salary").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists complexData")
    complexData.writeTo("complexData").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
    
    spark.sql("drop view if exists courseSales")
    courseSales.writeTo("courseSales").tableProperty("write.format.default", "parquet").
      using("iceberg").create()
  }
}
