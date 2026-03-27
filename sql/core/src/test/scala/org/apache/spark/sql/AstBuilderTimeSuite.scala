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

class AstBuilderTimeSuite extends QueryTest with SharedSparkSession {

  // 1. Basic literal
  test("TIME literal parsing") {
    val df = sql("SELECT TIME '12:30:00'")
    val t = df.collect().head.get(0).asInstanceOf[java.time.LocalTime]

    assert(t.getHour == 12)
    assert(t.getMinute == 30)
  }

  // 2. With microseconds
  test("TIME literal with microseconds") {
    val df = sql("SELECT TIME '12:30:00.123456'")
    val t = df.collect().head.get(0).asInstanceOf[java.time.LocalTime]

    assert(t.getNano == 123456000)
  }

  // 3. Invalid literal
  test("TIME literal invalid value") {
    intercept[Exception] {
      sql("SELECT TIME '25:00:00'").collect()
    }
  }

  // 4. Wrong format
  test("TIME literal invalid format") {
    intercept[Exception] {
      sql("SELECT TIME '12-30-00'").collect()
    }
  }
}
