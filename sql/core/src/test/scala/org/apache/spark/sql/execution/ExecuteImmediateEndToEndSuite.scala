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
package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ExecuteImmediateEndToEndSuite extends QueryTest with SharedSparkSession {

  test("SPARK-47033: EXECUTE IMMEDIATE USING does not recognize session variable names") {
    try {
      spark.sql("DECLARE parm = 'Hello';")

      val originalQuery = spark.sql(
        "EXECUTE IMMEDIATE 'SELECT :parm' USING system.session.parm AS parm;")
      val newQuery = spark.sql("EXECUTE IMMEDIATE 'SELECT :parm' USING system.session.parm;")

      assert(originalQuery.columns sameElements newQuery.columns)

      checkAnswer(originalQuery, newQuery.collect().toIndexedSeq)
    } finally {
      spark.sql("DROP TEMPORARY VARIABLE IF EXISTS parm;")
    }
  }
}
