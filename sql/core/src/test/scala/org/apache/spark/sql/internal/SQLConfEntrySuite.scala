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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.util.ThreadUtils

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

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.setConfString(key, "abc")
      },
      condition = "INVALID_CONF_VALUE.TYPE_MISMATCH",
      parameters = Map(
        "confName" -> key,
        "confValue" -> "abc",
        "confType" -> "int")
    )
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

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.setConfString(key, "abc")
      },
      condition = "INVALID_CONF_VALUE.TYPE_MISMATCH",
      parameters = Map(
        "confName" -> key,
        "confValue" -> "abc",
        "confType" -> "long")
    )
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
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.setConfString(key, "abc")
      },
      condition = "INVALID_CONF_VALUE.TYPE_MISMATCH",
      parameters = Map(
        "confName" -> key,
        "confValue" -> "abc",
        "confType" -> "boolean")
    )
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

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.setConfString(key, "abc")
      },
      condition = "INVALID_CONF_VALUE.TYPE_MISMATCH",
      parameters = Map(
        "confName" -> key,
        "confValue" -> "abc",
        "confType" -> "double")
    )
  }

  test("stringConf") {
    val key = "spark.sql.SQLConfEntrySuite.string"
    val confEntry = buildConf(key).stringConf.createWithDefault("")
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
    val candidates = Set("a", "b", "c")
    val confEntry = buildConf(key)
      .stringConf
      .checkValues(candidates)
      .createWithDefault("a")
    assert(conf.getConf(confEntry) === "a")

    conf.setConf(confEntry, "b")
    assert(conf.getConf(confEntry) === "b")

    conf.setConfString(key, "c")
    assert(conf.getConfString(key, "a") === "c")
    assert(conf.getConfString(key) === "c")
    assert(conf.getConf(confEntry) === "c")

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.setConfString(key, "abc")
      },
      condition = "INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS",
      parameters = Map(
        "confName" -> key,
        "confValue" -> "abc",
        "confOptions" -> candidates.mkString(", "))
    )
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
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.getConf(confEntry)
      },
      condition = "INVALID_CONF_VALUE.REQUIREMENT",
      parameters = Map(
        "confName" -> confEntry.key,
        "confValue" -> "-1",
        "confRequirement" -> "The maximum size of the cache must not be negative")
    )

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        conf.setConfString(confEntry.key, "-1")
      },
      condition = "INVALID_CONF_VALUE.REQUIREMENT",
      parameters = Map(
        "confName" -> confEntry.key,
        "confValue" -> "-1",
        "confRequirement" -> "The maximum size of the cache must not be negative")
    )
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

  test("getConfigEntries can be iterated while entries are registered") {
    // An overlapping iteration sees every entry registered before it began; later ones may vary.
    // Guards against an in-place map throwing ConcurrentModificationException mid-iteration.
    val prefix = "spark.sql.SQLConfEntrySuite.concurrent"
    val beforeKeys = (0 until 50).map(i => s"$prefix.before$i")
    val duringKeys = (0 until 200).map(i => s"$prefix.during$i")
    // Every entry this test registers, so cleanup unregisters exactly those.
    val registered = new ConcurrentLinkedQueue[ConfigEntry[_]]()
    val stopWriter = new AtomicBoolean(false)
    val iterating = new CountDownLatch(1)
    val firstRegistered = new CountDownLatch(1)
    var writer: Option[Future[Unit]] = None
    try {
      beforeKeys.foreach(key => registered.add(buildConf(key).intConf.createWithDefault(0)))
      writer = Some(Future {
        assert(iterating.await(1, TimeUnit.MINUTES))
        duringKeys.iterator.takeWhile(_ => !stopWriter.get()).foreach { key =>
          registered.add(buildConf(key).intConf.createWithDefault(0))
          firstRegistered.countDown()
        }
      }(ExecutionContext.global))
      val iterator = SQLConf.getConfigEntries().iterator()
      val seen = mutable.HashSet(iterator.next().key)
      iterating.countDown()
      // At least one registration happens while this iteration is open.
      assert(firstRegistered.await(1, TimeUnit.MINUTES))
      while (iterator.hasNext) seen += iterator.next().key
      beforeKeys.foreach(key => assert(seen.contains(key), key))
      ThreadUtils.awaitResult(writer.get, 1.minute)
      val after = SQLConf.getConfigEntries().asScala.map(_.key).toSet
      (beforeKeys ++ duringKeys).foreach(key => assert(after.contains(key), key))
    } finally {
      stopWriter.set(true)
      iterating.countDown()
      // The writer stops at its next key, so this join is bounded; it must finish before cleanup.
      writer.foreach(w => Try(ThreadUtils.awaitResult(w, Duration.Inf)))
      registered.asScala.foreach(SQLConf.unregister)
    }
  }

  test("registry lookups tolerate a null key") {
    assert(SQLConf.getConfigEntry(null) === null)
    assert(!SQLConf.containsConfigKey(null))
  }
}
