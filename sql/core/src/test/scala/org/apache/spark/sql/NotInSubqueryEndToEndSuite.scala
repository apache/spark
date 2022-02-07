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

class NotInSubqueryEndToEndSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  val t = "test_table"

  test("SPARK-38132: Avoid optimizing Not IN subquery") {
    withTable(t) {
      Seq[(Integer, Integer)](
        (1, 1),
        (2, 2),
        (3, 3),
        (4, null),
        (null, 0))
        .toDF("c1", "c2").write.saveAsTable(t)
      val df = spark.table(t)

      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) = true"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) = true"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) <=> true"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) <=> true"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t)) != false"), Seq.empty)
      checkAnswer(df.where(s"(c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) != false"),
        Row(4, null) :: Nil)
      checkAnswer(df.where(s"NOT((c1 NOT IN (SELECT c2 FROM $t)) <=> false)"), Seq.empty)
      checkAnswer(df.where(s"NOT((c1 NOT IN (SELECT c2 FROM $t WHERE c2 IS NOT NULL)) <=> false)"),
        Row(4, null) :: Nil)
    }
  }
}
