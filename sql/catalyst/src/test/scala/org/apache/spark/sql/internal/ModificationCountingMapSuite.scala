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

import java.util.function.{BiConsumer, BiFunction, Function => JFunction}

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually.{eventually, timeout}

import org.apache.spark.SparkFunSuite

class ModificationCountingMapSuite extends SparkFunSuite {

  test("modification count advances on every mutating call") {
    val map = new ModificationCountingMap
    assert(map.modificationCount == 0)

    map.get("a")
    map.containsKey("a")
    map.getOrDefault("a", "default")
    map.entrySet.size
    assert(map.modificationCount == 0)

    map.put("a", "av")
    assert(map.modificationCount == 1)
    map.put("a", "av")
    assert(map.modificationCount == 2)
    map.putAll(Map("b" -> "bv").asJava)
    assert(map.modificationCount == 3)
    map.remove("a")
    assert(map.modificationCount == 4)
    map.clear()
    assert(map.modificationCount == 5)
  }

  test("Map default methods and Scala wrapper mutators advance the modification count") {
    val appendNew: BiFunction[String, String, String] = (_, value) => s"$value-new"
    val removeEntry: BiFunction[String, String, String] = (_, _) => null
    val valueFromKey: JFunction[String, String] = key => s"$key-value"
    // (name, mutation of a map holding only "p" -> "v", expected entries afterwards)
    val cases: Seq[(String, ModificationCountingMap => Unit, Map[String, String])] = Seq(
      ("putIfAbsent", _.putIfAbsent("q", "w"), Map("p" -> "v", "q" -> "w")),
      ("compute", _.compute("p", appendNew), Map("p" -> "v-new")),
      ("compute to null", _.compute("p", removeEntry), Map.empty),
      ("computeIfAbsent", _.computeIfAbsent("q", valueFromKey),
        Map("p" -> "v", "q" -> "q-value")),
      ("computeIfPresent", _.computeIfPresent("p", appendNew), Map("p" -> "v-new")),
      ("merge", _.merge("p", "w", appendNew), Map("p" -> "w-new")),
      ("replace", _.replace("p", "w"), Map("p" -> "w")),
      ("replace if equal", _.replace("p", "v", "w"), Map("p" -> "w")),
      ("remove if equal", _.remove("p", "v"), Map.empty),
      ("replaceAll", _.replaceAll(appendNew), Map("p" -> "v-new")),
      ("asScala update", _.asScala.update("q", "w"), Map("p" -> "v", "q" -> "w")),
      ("asScala +=", _.asScala += ("q" -> "w"), Map("p" -> "v", "q" -> "w")),
      ("asScala -=", _.asScala -= "p", Map.empty),
      ("asScala clear", _.asScala.clear(), Map.empty))

    cases.foreach { case (name, mutate, expectedEntries) =>
      val map = new ModificationCountingMap
      map.put("p", "v")
      val countBefore = map.modificationCount

      mutate(map)

      assert(map.asScala == expectedEntries, name)
      assert(map.modificationCount > countBefore, name)
    }
  }

  test("snapshot pairs a copy of the entries with their modification count") {
    val map = new ModificationCountingMap
    map.put("a", "av")

    val (count, entries) = map.snapshotWithModificationCount
    map.put("b", "bv")

    assert(count == 1)
    assert(entries.asScala == Map("a" -> "av"))
    assert(map.modificationCount == 2)
  }

  test("every method except the views locks the map itself") {
    // Callers such as SQLConf.getAllConfs and RuntimeConfig.set hold this map's monitor to make a
    // sequence of calls atomic, which works only if every method takes that same monitor.
    val appendNew: BiFunction[String, String, String] = (_, value) => s"$value-new"
    val valueFromKey: JFunction[String, String] = key => s"$key-value"
    val ignoreEntry: BiConsumer[String, String] = (_, _) => ()
    val operations: Seq[(String, ModificationCountingMap => Any)] = Seq(
      ("put", _.put("p", "w")),
      ("remove", _.remove("p")),
      ("putAll", _.putAll(Map("q" -> "w").asJava)),
      ("clear", _.clear()),
      ("putIfAbsent", _.putIfAbsent("q", "w")),
      ("remove if equal", _.remove("p", "v")),
      ("replace", _.replace("p", "w")),
      ("replace if equal", _.replace("p", "v", "w")),
      ("compute", _.compute("p", appendNew)),
      ("computeIfAbsent", _.computeIfAbsent("q", valueFromKey)),
      ("computeIfPresent", _.computeIfPresent("p", appendNew)),
      ("merge", _.merge("p", "w", appendNew)),
      ("replaceAll", _.replaceAll(appendNew)),
      ("snapshotWithModificationCount", _.snapshotWithModificationCount),
      ("size", _.size),
      ("isEmpty", _.isEmpty),
      ("containsKey", _.containsKey("p")),
      ("containsValue", _.containsValue("v")),
      ("get", _.get("p")),
      ("getOrDefault", _.getOrDefault("q", "w")),
      ("forEach", _.forEach(ignoreEntry)),
      ("equals", _.equals(new java.util.HashMap[String, String])),
      ("hashCode", _.hashCode),
      ("toString", _.toString))

    operations.foreach { case (name, operation) =>
      val map = new ModificationCountingMap
      map.put("p", "v")
      val caller = new Thread(() => { operation(map); () }, s"ModificationCountingMap-$name")
      map.synchronized {
        caller.start()
        // A caller that takes the map's monitor cannot proceed while this thread holds it.
        eventually(timeout(10.seconds)) {
          assert(caller.getState == Thread.State.BLOCKED, name)
        }
      }
      caller.join(10000)
      assert(!caller.isAlive, name)
    }
  }

  test("key, value, and entry views are read-only") {
    val map = new ModificationCountingMap
    map.put("a", "av")

    intercept[UnsupportedOperationException](map.keySet.remove("a"))
    intercept[UnsupportedOperationException](map.values.clear())
    intercept[UnsupportedOperationException](map.entrySet.iterator.next().setValue("bv"))
    intercept[UnsupportedOperationException](map.entrySet.removeIf(_ => true))

    assert(map.get("a") == "av")
    assert(map.modificationCount == 1)
  }

  test("equals, hashCode, and toString follow the entries") {
    val map = new ModificationCountingMap
    map.put("a", "av")
    val plain = new java.util.HashMap[String, String]
    plain.put("a", "av")

    assert(map == plain)
    assert(map.hashCode == plain.hashCode)
    assert(map.toString == plain.toString)
  }
}
