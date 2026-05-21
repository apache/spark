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

  /**
   * Catalog stub that simulates implementations which validate the request (ACLs, properties,
   * etc.) before checking namespace existence — so `createNamespace` can fail on a pre-existing
   * namespace with an error other than `NamespaceAlreadyExistsException`.
   */
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
        throw new IllegalStateException("simulated authz/validation failure")
      }
    }

    override def listNamespaces(): Array[Array[String]] = Array.empty
    override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array.empty
    override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] =
      throw new NoSuchNamespaceException(namespace)
    override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {}
    override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = true
  }

  test("SPARK-55250: IF NOT EXISTS swallows underlying failure when namespace already exists") {
    val catalog = new TrackingNamespaceCatalog(existing = true, createFails = true)
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
    assert(catalog.createCalled.get(),
      "createNamespace is attempted before falling back to the existence check")
    assert(catalog.existsCalled.get(),
      "after createNamespace fails, namespaceExists confirms the no-op")
  }

  test("SPARK-55250: IF NOT EXISTS propagates underlying failure when namespace is absent") {
    val catalog = new TrackingNamespaceCatalog(existing = false, createFails = true)
    val e = intercept[IllegalStateException] {
      CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
    }
    assert(e.getMessage.contains("simulated authz/validation failure"))
    assert(catalog.createCalled.get())
    assert(catalog.existsCalled.get(),
      "namespaceExists is consulted to decide whether the failure is recoverable")
  }

  test("SPARK-55250: IF NOT EXISTS skips fallback when createNamespace succeeds") {
    val catalog = new TrackingNamespaceCatalog(existing = false, createFails = false)
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
    assert(catalog.createCalled.get())
    assert(!catalog.existsCalled.get(),
      "namespaceExists must not be called on the happy path — preserves SPARK-55250's 1-RPC win")
  }

  private class RacyNamespaceCatalog extends SupportsNamespaces {
    val existsCalled = new AtomicBoolean(false)
    override def name(): String = "racy"
    override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
    override def namespaceExists(namespace: Array[String]): Boolean = {
      existsCalled.set(true)
      false
    }
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

  test("SPARK-55250: IF NOT EXISTS swallows NamespaceAlreadyExistsException without fallback") {
    val catalog = new RacyNamespaceCatalog
    CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = true, Map.empty).executeCollect()
    assert(!catalog.existsCalled.get(),
      "NamespaceAlreadyExistsException is handled directly without a second RPC")
  }

  test("SPARK-55250: without IF NOT EXISTS, createNamespace is called and errors propagate") {
    val catalog = new TrackingNamespaceCatalog(existing = true, createFails = true)
    intercept[IllegalStateException] {
      CreateNamespaceExec(catalog, Seq("ns"), ifNotExists = false, Map.empty).executeCollect()
    }
    assert(catalog.createCalled.get())
    assert(!catalog.existsCalled.get(),
      "namespaceExists fallback only applies when IF NOT EXISTS is set")
  }
}
