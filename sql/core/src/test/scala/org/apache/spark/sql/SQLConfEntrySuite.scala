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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SQLConf._

class SQLConfEntrySuite extends SparkFunSuite {

  val conf = new SQLConf

  test("intConf") {
    val key = "spark.sql.SQLConfEntrySuite.int"
    val confEntry = SQLConfEntry.intConf(key)
    assert(conf.getConf(confEntry, 5) === 5)

    conf.setConf(confEntry, 10)
    assert(conf.getConf(confEntry, 5) === 10)

    conf.setRawConf(key, "20")
    assert(conf.getRawConf(key, "5") === "20")
    assert(conf.getRawConf(key) === "20")
    assert(conf.getConf(confEntry, 5) === 20)

    val e = intercept[IllegalArgumentException] {
      conf.setRawConf(key, "abc")
    }
    assert(e.getMessage === s"$key should be int, but was abc")
  }

  test("longConf") {
    val key = "spark.sql.SQLConfEntrySuite.long"
    val confEntry = SQLConfEntry.longConf(key)
    assert(conf.getConf(confEntry, 5L) === 5L)

    conf.setConf(confEntry, 10L)
    assert(conf.getConf(confEntry, 5L) === 10L)

    conf.setRawConf(key, "20")
    assert(conf.getRawConf(key, "5") === "20")
    assert(conf.getRawConf(key) === "20")
    assert(conf.getConf(confEntry, 5L) === 20L)

    val e = intercept[IllegalArgumentException] {
      conf.setRawConf(key, "abc")
    }
    assert(e.getMessage === s"$key should be long, but was abc")
  }

  test("booleanConf") {
    val key = "spark.sql.SQLConfEntrySuite.boolean"
    val confEntry = SQLConfEntry.booleanConf(key)
    assert(conf.getConf(confEntry, false) === false)

    conf.setConf(confEntry, true)
    assert(conf.getConf(confEntry, false) === true)

    conf.setRawConf(key, "true")
    assert(conf.getRawConf(key, "false") === "true")
    assert(conf.getRawConf(key) === "true")
    assert(conf.getConf(confEntry, false) === true)

    val e = intercept[IllegalArgumentException] {
      conf.setRawConf(key, "abc")
    }
    assert(e.getMessage === s"$key should be boolean, but was abc")
  }

  test("floatConf") {
    val key = "spark.sql.SQLConfEntrySuite.float"
    val confEntry = SQLConfEntry.floatConf(key)
    assert(conf.getConf(confEntry, 5.0F) === 5.0F)

    conf.setConf(confEntry, 10.0F)
    assert(conf.getConf(confEntry, 5.0F) === 10.0F)

    conf.setRawConf(key, "20.0")
    assert(conf.getRawConf(key, "5.0") === "20.0")
    assert(conf.getRawConf(key) === "20.0")
    assert(conf.getConf(confEntry, 5.0F) === 20.0F)

    val e = intercept[IllegalArgumentException] {
      conf.setRawConf(key, "abc")
    }
    assert(e.getMessage === s"$key should be float, but was abc")
  }

  test("doubleConf") {
    val key = "spark.sql.SQLConfEntrySuite.double"
    val confEntry = SQLConfEntry.doubleConf(key)
    assert(conf.getConf(confEntry, 5.0) === 5.0)

    conf.setConf(confEntry, 10.0)
    assert(conf.getConf(confEntry, 5.0) === 10.0)

    conf.setRawConf(key, "20.0")
    assert(conf.getRawConf(key, "5.0") === "20.0")
    assert(conf.getRawConf(key) === "20.0")
    assert(conf.getConf(confEntry, 5.0) === 20.0)

    val e = intercept[IllegalArgumentException] {
      conf.setRawConf(key, "abc")
    }
    assert(e.getMessage === s"$key should be double, but was abc")
  }

  test("stringConf") {
    val key = "spark.sql.SQLConfEntrySuite.string"
    val confEntry = SQLConfEntry.stringConf(key)
    assert(conf.getConf(confEntry, "abc") === "abc")

    conf.setConf(confEntry, "abcd")
    assert(conf.getConf(confEntry, "abc") === "abcd")

    conf.setRawConf(key, "abcde")
    assert(conf.getRawConf(key, "abc") === "abcde")
    assert(conf.getRawConf(key) === "abcde")
    assert(conf.getConf(confEntry, "abc") === "abcde")
  }

  test("seqConf") {
    val key = "spark.sql.SQLConfEntrySuite.seq"
    val confEntry = SQLConfEntry.seqConf("spark.sql.SQLConfEntrySuite.seq", { v =>
      try {
        v.toInt
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"items of $key should be int, but was $v")
      }
    })
    assert(conf.getConf(confEntry, Seq(1, 2, 3)) == Seq(1, 2, 3))

    conf.setConf(confEntry, Seq(1, 2, 3, 4))
    assert(conf.getConf(confEntry, Seq(1, 2, 3)) === Seq(1, 2, 3, 4))

    conf.setRawConf(key, "1,2,3,4,5")
    assert(conf.getRawConf(key, "1,2,3") === "1,2,3,4,5")
    assert(conf.getRawConf(key) === "1,2,3,4,5")
    assert(conf.getConf(confEntry, Seq(1, 2, 3)) === Seq(1, 2, 3, 4, 5))

    val e = intercept[IllegalArgumentException] {
      conf.setRawConf(key, "a,b,c")
    }
    assert(e.getMessage === s"items of $key should be int, but was a")
  }
}
