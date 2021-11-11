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

package org.apache.spark.examples.extensions

import org.apache.spark.sql.SparkSession

/**
 * [[SessionExtensionsWithLoader]] is registered in
 * src/main/resources/META-INF/services/org.apache.spark.sql.SparkSessionExtensionsProvider
 *
 * [[SessionExtensionsWithoutLoader]] is registered via spark.sql.extensions
 */
object SparkSessionExtensionsTest {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSessionExtensionsTest")
      .config("spark.sql.extensions", classOf[SessionExtensionsWithoutLoader].getName)
      .getOrCreate()
    spark.sql("SELECT age_one('2018-11-17'), age_two('2018-11-17')").show()
  }
}
