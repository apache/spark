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
package org.apache.spark.sql.connect.client.arrow

import java.io.File
import java.net.URLClassLoader
import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier}

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.sql.connect.test.ConnectFunSuite

/**
 * Concurrency guard for the synchronized reflection in [[ArrowDeserializers]]:
 * [[ArrowDeserializers.resolveCompanionFromMirror]] (collection companions) and
 * [[ArrowDeserializers.resolveEnumFromMirror]] (Scala `Enumeration` modules).
 *
 * Scala runtime reflection is not thread-safe (scala/bug#6240): resolving a symbol via
 * `mirror.classSymbol(cls).companion/.module.asModule` can, under concurrent access, observe the
 * symbol as `NoSymbol`, so `.asModule` throws `ScalaReflectionException: <none> is not a module`.
 * Both `*FromMirror` methods serialize the reflection through a single monitor to prevent it.
 *
 * The race only manifests while a mirror's symbol table is cold, so each repetition builds a fresh
 * mirror over a classloader parented at the platform loader (so `scala.*` is reloaded cold) and
 * drives the real synchronized method from several threads released at once. With the
 * synchronization this is reliably green; without it the suite races red.
 */
class ArrowDeserializersConcurrencySuite extends ConnectFunSuite {

  private val collectionClassNames = Seq(
    "scala.collection.immutable.List",
    "scala.collection.immutable.Vector",
    "scala.collection.immutable.Set",
    "scala.collection.immutable.Map",
    "scala.collection.mutable.ArrayBuffer",
    "scala.collection.mutable.HashMap")

  // `object Foo extends Enumeration` compiles to a module class `Foo$`, the form production passes
  // to `resolveEnum`. Declared top-level (below) so a cold classloader can load them by name.
  private val enumClassNames =
    (0 until 4).map(i => s"org.apache.spark.sql.connect.client.arrow.ConcurrencyTestEnum$i$$")

  /** A fresh classloader parented at the platform loader, so `scala.*` is reloaded cold. */
  private def newColdLoader(): URLClassLoader = {
    val urls = System.getProperty("java.class.path")
      .split(File.pathSeparator)
      .filter(_.nonEmpty)
      .map(p => new File(p).toURI.toURL)
    new URLClassLoader(urls, ClassLoader.getPlatformClassLoader)
  }

  // Drive `resolve` against a fresh cold mirror from 8 threads, 50 times; fail on any error/hang.
  private def hammer(names: Seq[String], resolve: (ru.Mirror, Class[_]) => Any): Unit = {
    val errors = new ConcurrentLinkedQueue[Throwable]()
    for (_ <- 0 until 50) {
      // Fresh cold classloader + mirror per repetition re-opens the reflection race window.
      val loader = newColdLoader()
      val mirror = ru.runtimeMirror(loader)
      val classes = names.map(loader.loadClass)
      val barrier = new CyclicBarrier(8)
      val threads = (0 until 8).map { _ =>
        new Thread(() => {
          barrier.await() // release all threads simultaneously onto the cold mirror
          classes.foreach { cls =>
            try resolve(mirror, cls)
            catch { case e: Throwable => errors.add(e) }
          }
        })
      }
      threads.foreach(_.start())
      threads.foreach { t =>
        t.join(60000)
        // A still-alive thread means a hang (e.g. a deadlock from a botched lock change); fail
        // loudly instead of letting an empty `errors` queue read as success.
        assert(!t.isAlive, "thread did not finish within 60s (possible deadlock)")
      }
    }
    assert(
      errors.isEmpty,
      s"reflection raced under concurrent access (${errors.size} error(s)): " +
        errors.asScala.map(e => s"${e.getClass.getName}: ${e.getMessage}").toSet.mkString("; "))
  }

  test("SPARK-57371: resolveCompanion is thread-safe under concurrent cold-mirror access") {
    hammer(collectionClassNames, (m, c) => ArrowDeserializers.resolveCompanionFromMirror(m, c))
  }

  test("SPARK-57371: resolveEnum is thread-safe under concurrent cold-mirror access") {
    hammer(enumClassNames, (m, c) => ArrowDeserializers.resolveEnumFromMirror(m, c))
  }
}

// Enumeration fixtures for the enum concurrency test; keep the count in sync with `enumClassNames`.
private[arrow] object ConcurrencyTestEnum0 extends Enumeration { val A, B, C = Value }
private[arrow] object ConcurrencyTestEnum1 extends Enumeration { val A, B, C = Value }
private[arrow] object ConcurrencyTestEnum2 extends Enumeration { val A, B, C = Value }
private[arrow] object ConcurrencyTestEnum3 extends Enumeration { val A, B, C = Value }
