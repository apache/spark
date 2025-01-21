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

import org.apache.spark.sql.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.unsafe.types.VariantVal

class SQLExpressionsSuite extends QueryTest with RemoteSparkSession {

  test("variants") {
    val topLevelVariants = spark.sql("select parse_json(id::string) from range(10)")
    checkAnswer(
      topLevelVariants,
      (0 until 10)
        .map(i => Row(new VariantVal(Array[Byte](12, i.toByte), Array[Byte](1, 0, 0)))))
    val structsOfVariants = spark.sql("select struct(parse_json(id::string)) from range(10)")
    checkAnswer(
      structsOfVariants,
      (0 until 10)
        .map(i => Row(Row(new VariantVal(Array[Byte](12, i.toByte), Array[Byte](1, 0, 0))))))
    val arraysOfVariants = spark.sql("select array(parse_json(id::string)) from range(10)")
    checkAnswer(
      arraysOfVariants,
      (0 until 10)
        .map(i => Row(Seq(new VariantVal(Array[Byte](12, i.toByte), Array[Byte](1, 0, 0))))))
    val mapsOfVariants = spark.sql("select map(id, parse_json(id::string)) from range(10)")
    checkAnswer(
      mapsOfVariants,
      (0 until 10)
        .map(i => Row(Map((i, new VariantVal(Array[Byte](12, i.toByte), Array[Byte](1, 0, 0)))))))
  }
}
