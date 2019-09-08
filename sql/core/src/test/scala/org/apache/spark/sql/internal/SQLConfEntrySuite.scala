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

package org.apache.spark.sql.internal

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.internal.SQLConf._

class SQLConfEntrySuite extends SparkFunSuite {

  val conf = new SQLConf

  test("intConf") {
    val key = "spark.sql.SQLConfEntrySuite.int"
    val confEntry = buildConf(key).intConf.createWithDefault(1)
    assert(conf.getConf(confEntry, 5) === 5)

    conf.setConf(confEntry, 10)
    assert(conf.getConf(confEntry, 5) === 10)

    conf.setConfString(key, "20")
    assert(conf.getConfString(key, "5") === "20")
    assert(conf.getConfString(key) === "20")
    assert(conf.getConf(confEntry, 5) === 20)

    conf.setConfString(key, " 20")
    assert(conf.getConf(confEntry, 5) === 20)

    val e = intercept[IllegalArgumentException] {
      conf.setConfString(key, "abc")
    }
    assert(e.getMessage === s"$key should be int, but was abc")
  }

  test("longConf") {
    val key = "spark.sql.SQLConfEntrySuite.long"
    val confEntry = buildConf(key).longConf.createWithDefault(1L)
    assert(conf.getConf(confEntry, 5L) === 5L)

    conf.setConf(confEntry, 10L)
    assert(conf.getConf(confEntry, 5L) === 10L)

    conf.setConfString(key, "20")
    assert(conf.getConfString(key, "5") === "20")
    assert(conf.getConfString(key) === "20")
    assert(conf.getConf(confEntry, 5L) === 20L)

    val e = intercept[IllegalArgumentException] {
      conf.setConfString(key, "abc")
    }
    assert(e.getMessage === s"$key should be long, but was abc")
  }

  test("booleanConf") {
    val key = "spark.sql.SQLConfEntrySuite.boolean"
    val confEntry = buildConf(key).booleanConf.createWithDefault(true)
    assert(conf.getConf(confEntry, false) === false)

    conf.setConf(confEntry, true)
    assert(conf.getConf(confEntry, false))

    conf.setConfString(key, "true")
    assert(conf.getConfString(key, "false") === "true")
    assert(conf.getConfString(key) === "true")
    assert(conf.getConf(confEntry, false))

    conf.setConfString(key, " true ")
    assert(conf.getConf(confEntry, false))
    val e = intercept[IllegalArgumentException] {
      conf.setConfString(key, "abc")
    }
    assert(e.getMessage === s"$key should be boolean, but was abc")
  }

  test("doubleConf") {
    val key = "spark.sql.SQLConfEntrySuite.double"
    val confEntry = buildConf(key).doubleConf.createWithDefault(1d)
    assert(conf.getConf(confEntry, 5.0) === 5.0)

    conf.setConf(confEntry, 10.0)
    assert(conf.getConf(confEntry, 5.0) === 10.0)

    conf.setConfString(key, "20.0")
    assert(conf.getConfString(key, "5.0") === "20.0")
    assert(conf.getConfString(key) === "20.0")
    assert(conf.getConf(confEntry, 5.0) === 20.0)

    val e = intercept[IllegalArgumentException] {
      conf.setConfString(key, "abc")
    }
    assert(e.getMessage === s"$key should be double, but was abc")
  }

  test("stringConf") {
    val key = "spark.sql.SQLConfEntrySuite.string"
    val confEntry = buildConf(key).stringConf.createWithDefault(null)
    assert(conf.getConf(confEntry, "abc") === "abc")

    conf.setConf(confEntry, "abcd")
    assert(conf.getConf(confEntry, "abc") === "abcd")

    conf.setConfString(key, "abcde")
    assert(conf.getConfString(key, "abc") === "abcde")
    assert(conf.getConfString(key) === "abcde")
    assert(conf.getConf(confEntry, "abc") === "abcde")
  }

  test("enumConf") {
    val key = "spark.sql.SQLConfEntrySuite.enum"
    val confEntry = buildConf(key)
      .stringConf
      .checkValues(Set("a", "b", "c"))
      .createWithDefault("a")
    assert(conf.getConf(confEntry) === "a")

    conf.setConf(confEntry, "b")
    assert(conf.getConf(confEntry) === "b")

    conf.setConfString(key, "c")
    assert(conf.getConfString(key, "a") === "c")
    assert(conf.getConfString(key) === "c")
    assert(conf.getConf(confEntry) === "c")

    val e = intercept[IllegalArgumentException] {
      conf.setConfString(key, "d")
    }
    assert(e.getMessage === s"The value of $key should be one of a, b, c, but was d")
  }

  test("stringSeqConf") {
    val key = "spark.sql.SQLConfEntrySuite.stringSeq"
    val confEntry = buildConf(key)
      .stringConf
      .toSequence
      .createWithDefault(Nil)
    assert(conf.getConf(confEntry, Seq("a", "b", "c")) === Seq("a", "b", "c"))

    conf.setConf(confEntry, Seq("a", "b", "c", "d"))
    assert(conf.getConf(confEntry, Seq("a", "b", "c")) === Seq("a", "b", "c", "d"))

    conf.setConfString(key, "a,b,c,d,e")
    assert(conf.getConfString(key, "a,b,c") === "a,b,c,d,e")
    assert(conf.getConfString(key) === "a,b,c,d,e")
    assert(conf.getConf(confEntry, Seq("a", "b", "c")) === Seq("a", "b", "c", "d", "e"))
  }

  test("optionalConf") {
    val key = "spark.sql.SQLConfEntrySuite.optional"
    val confEntry = buildConf(key)
      .stringConf
      .createOptional

    assert(conf.getConf(confEntry) === None)
    conf.setConfString(key, "a")
    assert(conf.getConf(confEntry) === Some("a"))
  }

  test("duplicate entry") {
    val key = "spark.sql.SQLConfEntrySuite.duplicate"
    buildConf(key).stringConf.createOptional
    intercept[IllegalArgumentException] {
      buildConf(key).stringConf.createOptional
    }
  }

  test("StaticSQLConf.FILESOURCE_TABLE_RELATION_CACHE_SIZE") {
    val confEntry = StaticSQLConf.FILESOURCE_TABLE_RELATION_CACHE_SIZE
    assert(conf.getConf(confEntry) === 1000)

    conf.setConf(confEntry, -1)
    val e1 = intercept[IllegalArgumentException] {
      conf.getConf(confEntry)
    }
    assert(e1.getMessage === "The maximum size of the cache must not be negative")

    val e2 = intercept[IllegalArgumentException] {
      conf.setConfString(confEntry.key, "-1")
    }
    assert(e2.getMessage === "The maximum size of the cache must not be negative")
  }

  test("clone SQLConf") {
    val original = new SQLConf
    val key = "spark.sql.SQLConfEntrySuite.clone"
    assert(original.getConfString(key, "noentry") === "noentry")

    // inheritance
    original.setConfString(key, "orig")
    val clone = original.clone()
    assert(original ne clone)
    assert(clone.getConfString(key, "noentry") === "orig")

    // independence
    clone.setConfString(key, "clone")
    assert(original.getConfString(key, "noentry") === "orig")
    original.setConfString(key, "dontcopyme")
    assert(clone.getConfString(key, "noentry") === "clone")
  }
}
