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

package org.apache.spark.sql.execution.datasources.v2

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException}
import org.apache.spark.sql.connector.catalog.{NamespaceChange, SupportsNamespaces}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CreateNamespaceExecSuite extends SparkFunSuite {

  private class TrackingNamespaceCatalog(
      existing: Boolean,
      createFails: Boolean) extends SupportsNamespaces {
    val createCalled = new AtomicBoolean(false)
    val existsCalled = new AtomicBoolean(false)

    override def name(): String = "tracking"
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

    override def namespaceExists(namespace: Array[String]): Boolean = {
      existsCalled.set(true)
      existing
    }

    override def createNamespace(
        namespace: Array[String],
        metadata: util.Map[String, String]): Unit = {
      createCalled.set(true)
      if (createFails) {
        // Simulate a catalog that fails the create call for reasons unrelated to
        // existence (e.g. property validation), so that the test asserts we never
        // reach this code path when IF NOT EXISTS short-circuits.
        throw new IllegalStateException("createNamespace should not have been called")
      }
    }

    override def listNamespaces(): Array[Array[String]] = Array.empty
    override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array.empty
    override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
      throw new NoSuchNamespaceException(namespace)
    override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {}
    override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = true
  }

  test("SPARK-55250: IF NOT EXISTS skips createNamespace when the namespace already exists") {
    val catalog = new TrackingNamespaceCatalog(existing = true, createFails = true)
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
    assert(catalog.existsCalled.get(), "namespaceExists should be checked first")
    assert(!catalog.createCalled.get(),
      "createNamespace must not be invoked when IF NOT EXISTS and namespace already exists")
  }

  test("SPARK-55250: IF NOT EXISTS calls createNamespace when the namespace is absent") {
    val catalog = new TrackingNamespaceCatalog(existing = false, createFails = false)
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
    assert(catalog.createCalled.get())
  }

  test("SPARK-55250: IF NOT EXISTS swallows concurrent NamespaceAlreadyExistsException") {
    val catalog = new SupportsNamespaces {
      override def name(): String = "racy"
      override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
      override def namespaceExists(namespace: Array[String]): Boolean = false
      override def createNamespace(
          namespace: Array[String],
          metadata: util.Map[String, String]): Unit =
        throw new NamespaceAlreadyExistsException(namespace)
      override def listNamespaces(): Array[Array[String]] = Array.empty
      override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array.empty
      override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
        throw new NoSuchNamespaceException(namespace)
      override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {}
      override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = true
    }
    // Should not throw — IF NOT EXISTS swallows the race-induced AlreadyExists.
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
  }

  test("SPARK-55250: without IF NOT EXISTS, createNamespace is always called") {
    val catalog = new TrackingNamespaceCatalog(existing = true, createFails = false)
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = false, Map.empty).executeCollect()
    assert(catalog.createCalled.get(),
      "createNamespace must be invoked when IF NOT EXISTS is not set")
    assert(!catalog.existsCalled.get(),
      "namespaceExists must not be checked when IF NOT EXISTS is not set")
  }
}
