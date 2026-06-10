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
import java.net.{URL, URLClassLoader}
import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier}

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.{universe => ru}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.test.ConnectFunSuite

/**
 * Concurrency guard for [[ArrowDeserializers.resolveCompanionFromMirror]], the synchronized
 * reflection behind [[ArrowDeserializers.resolveCompanion]].
 *
 * Scala runtime reflection is not thread-safe (scala/bug#6240). Resolving a collection companion
 * via `mirror.classSymbol(cls).companion.asModule` can, under concurrent access, observe
 * `.companion` as `NoSymbol`, so `.asModule` throws `ScalaReflectionException: <none> is not a
 * module` -- the production symptom this fix addresses. `resolveCompanionFromMirror` guards
 * against it by serializing the reflection through a single monitor.
 *
 * The race only manifests while a mirror's symbol table is still cold. The `currentMirror` used
 * in production warms up after the first resolution, so the race window is effectively one-shot
 * per JVM and unreachable through the normal path. This suite re-opens it deterministically:
 * each repetition builds a runtime mirror over a FRESH classloader (parent = the platform loader,
 * so `scala.*` collection classes are reloaded fresh rather than delegating to the warm
 * application loader), giving a brand new, COLD mirror, and drives the real synchronized
 * `resolveCompanionFromMirror` against it from many threads at once.
 *
 * With the synchronization in place this is reliably green. If it is ever removed, concurrent
 * cold-mirror resolution races and the suite goes flaky/red -- the regression signal we keep
 * in CI.
 */
class ArrowDeserializersConcurrencySuite extends ConnectFunSuite with Logging {

  // Collection types whose companions are resolved by `resolveCompanion` in production. Breadth
  // maximizes concurrent symbol-table completion, which is what surfaces the race.
  private val collectionClassNames: Seq[String] = Seq(
    "scala.collection.immutable.List",
    "scala.collection.immutable.Vector",
    "scala.collection.immutable.Seq",
    "scala.collection.immutable.IndexedSeq",
    "scala.collection.immutable.LinearSeq",
    "scala.collection.immutable.Iterable",
    "scala.collection.immutable.Set",
    "scala.collection.immutable.HashSet",
    "scala.collection.immutable.TreeSet",
    "scala.collection.immutable.SortedSet",
    "scala.collection.immutable.BitSet",
    "scala.collection.immutable.ListSet",
    "scala.collection.immutable.Map",
    "scala.collection.immutable.HashMap",
    "scala.collection.immutable.TreeMap",
    "scala.collection.immutable.SortedMap",
    "scala.collection.immutable.ListMap",
    "scala.collection.immutable.Queue",
    "scala.collection.immutable.LazyList",
    "scala.collection.immutable.ArraySeq",
    "scala.collection.mutable.ArrayBuffer",
    "scala.collection.mutable.ListBuffer",
    "scala.collection.mutable.Buffer",
    "scala.collection.mutable.Seq",
    "scala.collection.mutable.IndexedSeq",
    "scala.collection.mutable.ArraySeq",
    "scala.collection.mutable.Queue",
    "scala.collection.mutable.Stack",
    "scala.collection.mutable.HashSet",
    "scala.collection.mutable.LinkedHashSet",
    "scala.collection.mutable.TreeSet",
    "scala.collection.mutable.BitSet",
    "scala.collection.mutable.Set",
    "scala.collection.mutable.Map",
    "scala.collection.mutable.HashMap",
    "scala.collection.mutable.LinkedHashMap",
    "scala.collection.mutable.TreeMap",
    "scala.collection.mutable.WeakHashMap",
    "scala.collection.mutable.ArrayDeque",
    "scala.collection.mutable.PriorityQueue",
    "scala.collection.mutable.UnrolledBuffer")

  /** Every jar/dir on the running classpath, so a cold loader can resolve any class fresh. */
  private val classpathUrls: Array[URL] = {
    val fromProp = Option(System.getProperty("java.class.path")).toSeq
      .flatMap(_.split(File.pathSeparator))
      .filter(_.nonEmpty)
      .map(p => new File(p).toURI.toURL)
    val fromLoaders = Iterator
      .iterate(getClass.getClassLoader)(_.getParent)
      .takeWhile(_ != null)
      .collect { case u: URLClassLoader => u.getURLs.toSeq }
      .flatten
      .toSeq
    (fromProp ++ fromLoaders).distinct.toArray
  }

  /** A fresh classloader whose parent is the platform loader, so scala.* is reloaded cold. */
  private def newColdLoader(): URLClassLoader =
    new URLClassLoader(classpathUrls, ClassLoader.getPlatformClassLoader)

  /** Class names that `resolveCompanion` handles cleanly single-threaded (on a cold mirror). */
  private def resolvableClassNames(): Seq[String] = {
    val loader = newColdLoader()
    val mirror = ru.runtimeMirror(loader)
    collectionClassNames.filter { name =>
      try {
        ArrowDeserializers.resolveCompanionFromMirror(mirror, loader.loadClass(name))
        true
      } catch {
        case _: Throwable => false
      }
    }
  }

  test("SPARK-57371: resolveCompanion is thread-safe under concurrent cold-mirror access") {
    val names = resolvableClassNames()
    assert(names.nonEmpty, "expected at least one resolvable collection companion")
    logInfo(s"Exercising resolveCompanion for ${names.size} collection companions")

    val reps = 50
    val threadsPerRep = 16
    val errors = new ConcurrentLinkedQueue[Throwable]()
    for (_ <- 0 until reps) {
      // Fresh cold classloader + mirror per repetition re-opens the reflection race window.
      val loader = newColdLoader()
      val mirror = ru.runtimeMirror(loader)
      // Pre-load the classes so per-classloader (synchronized) class loading stays out of the
      // concurrent hot path; only the racy reflection runs under contention.
      val classes = names.map(loader.loadClass).toArray
      val barrier = new CyclicBarrier(threadsPerRep)
      val threads = (0 until threadsPerRep).map { _ =>
        new Thread(() => {
          try {
            barrier.await() // release all threads simultaneously onto the cold mirror
            var i = 0
            while (i < classes.length) {
              try ArrowDeserializers.resolveCompanionFromMirror(mirror, classes(i))
              catch { case e: Throwable => errors.add(e) }
              i += 1
            }
          } catch {
            case e: Throwable => errors.add(e)
          }
        })
      }
      threads.foreach(_.start())
      threads.foreach(_.join(60000))
    }

    if (!errors.isEmpty) {
      val summary = errors.asScala
        .groupBy(e => s"${e.getClass.getName}: ${e.getMessage}")
        .map { case (k, v) => s"  x${v.size}  $k" }
        .toSeq
        .sorted
        .mkString("\n")
      fail(
        s"resolveCompanion raced under concurrent access: ${errors.size} error(s).\n$summary",
        errors.peek())
    }
  }
}
