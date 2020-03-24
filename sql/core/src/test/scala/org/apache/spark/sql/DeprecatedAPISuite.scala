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

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DeprecatedAPISuite extends QueryTest with SharedSparkSession {

  test("SQLContext.applySchema") {
    val rowRdd = sparkContext.parallelize(Seq(Row("Jack", 20), Row("Marry", 18)))
    val schema = StructType(StructField("name", StringType, false) ::
      StructField("age", IntegerType, true) :: Nil)
    val sqlContext = spark.sqlContext
    checkAnswer(sqlContext.applySchema(rowRdd, schema), Row("Jack", 20) :: Row("Marry", 18) :: Nil)
    checkAnswer(sqlContext.applySchema(rowRdd.toJavaRDD(), schema),
      Row("Jack", 20) :: Row("Marry", 18) :: Nil)
  }

  test("SQLContext.parquetFile") {
    val sqlContext = spark.sqlContext
    withTempDir { dir =>
      val parquetFile = s"${dir.toString}/${System.currentTimeMillis()}"
      val expectDF = spark.range(10).toDF()
      expectDF.write.parquet(parquetFile)
      val parquetDF = sqlContext.parquetFile(parquetFile)
      checkAnswer(parquetDF, expectDF)
    }
  }

  test("SQLContext.jsonFile") {
    val sqlContext = spark.sqlContext
    withTempDir { dir =>
      val jsonFile = s"${dir.toString}/${System.currentTimeMillis()}"
      val expectDF = spark.range(10).toDF()
      expectDF.write.json(jsonFile)
      var jsonDF = sqlContext.jsonFile(jsonFile)
      checkAnswer(jsonDF, expectDF)
      assert(jsonDF.schema === expectDF.schema.asNullable)

      var schema = expectDF.schema
      jsonDF = sqlContext.jsonFile(jsonFile, schema)
      checkAnswer(jsonDF, expectDF)
      assert(jsonDF.schema === schema.asNullable)

      jsonDF = sqlContext.jsonFile(jsonFile, 0.9)
      checkAnswer(jsonDF, expectDF)

      val jsonRDD = sparkContext.parallelize(Seq("{\"name\":\"Jack\",\"age\":20}",
        "{\"name\":\"Marry\",\"age\":18}"))
      jsonDF = sqlContext.jsonRDD(jsonRDD)
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD.toJavaRDD())
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)

      schema = StructType(StructField("name", StringType, false) ::
        StructField("age", IntegerType, false) :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD, schema)
      checkAnswer(jsonDF, Row("Jack", 20) :: Row("Marry", 18) :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD.toJavaRDD(), schema)
      checkAnswer(jsonDF, Row("Jack", 20) :: Row("Marry", 18) :: Nil)


      jsonDF = sqlContext.jsonRDD(jsonRDD, 0.9)
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)
      jsonDF = sqlContext.jsonRDD(jsonRDD.toJavaRDD(), 0.9)
      checkAnswer(jsonDF, Row(18, "Marry") :: Row(20, "Jack") :: Nil)
    }
  }

  test("SQLContext.load") {
    withTempDir { dir =>
      val path = s"${dir.toString}/${System.currentTimeMillis()}"
      val expectDF = spark.range(10).toDF()
      expectDF.write.parquet(path)
      val sqlContext = spark.sqlContext

      var loadDF = sqlContext.load(path)
      checkAnswer(loadDF, expectDF)

      loadDF = sqlContext.load(path, "parquet")
      checkAnswer(loadDF, expectDF)

      loadDF = sqlContext.load("parquet", Map("path" -> path))
      checkAnswer(loadDF, expectDF)

      loadDF = sqlContext.load("parquet", expectDF.schema, Map("path" -> path))
      checkAnswer(loadDF, expectDF)
    }
  }
}
