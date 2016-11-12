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

package org.apache.spark.sql.hive

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveVariableSubstitutionSuite extends QueryTest with TestHiveSingleton {
  test("SET hivevar with prefix") {
    spark.sql("SET hivevar:county=gram")
    assert(spark.conf.getOption("county") === Some("gram"))
  }

  test("SET hivevar with dotted name") {
    spark.sql("SET hivevar:eloquent.mosquito.alphabet=zip")
    assert(spark.conf.getOption("eloquent.mosquito.alphabet") === Some("zip"))
  }

  test("hivevar substitution") {
    spark.conf.set("pond", "bus")
    checkAnswer(spark.sql("SELECT '${hivevar:pond}'"), Row("bus") :: Nil)
  }

  test("variable substitution without a prefix") {
    spark.sql("SET hivevar:flask=plaid")
    checkAnswer(spark.sql("SELECT '${flask}'"), Row("plaid") :: Nil)
  }

  test("variable substitution precedence") {
    spark.conf.set("turn.aloof", "questionable")
    spark.sql("SET hivevar:turn.aloof=dime")
    // hivevar clobbers the conf setting
    checkAnswer(spark.sql("SELECT '${turn.aloof}'"), Row("dime") :: Nil)
  }
}
