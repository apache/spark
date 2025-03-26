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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.analysis.{NoSuchFunctionException, NoSuchNamespaceException}
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction
import org.apache.spark.sql.connector.catalog.procedures.UnboundProcedure

class InMemoryCatalog extends InMemoryTableCatalog with FunctionCatalog with ProcedureCatalog {
  protected val functions: util.Map[Identifier, UnboundFunction] =
    new ConcurrentHashMap[Identifier, UnboundFunction]()

  override protected def allNamespaces: Seq[Seq[String]] =
    (super.allNamespaces ++ functions.keySet.asScala.map(_.namespace.toSeq)).distinct

  override def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (namespace.isEmpty || namespaceExists(namespace)) {
      functions.keySet.asScala.filter(_.namespace.sameElements(namespace)).toArray
    } else {
      throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadFunction(ident: Identifier): UnboundFunction = {
    Option(functions.get(ident)) match {
      case Some(func) =>
        func
      case _ =>
        throw new NoSuchFunctionException(ident)
    }
  }

  def createFunction(ident: Identifier, fn: UnboundFunction): UnboundFunction = {
    functions.put(ident, fn)
  }

  def dropFunction(ident: Identifier): Unit = {
    functions.remove(ident)
  }

  def clearFunctions(): Unit = {
    functions.clear()
  }

  def createProcedure(ident: Identifier, procedure: UnboundProcedure): UnboundProcedure = {
    procedures.put(ident, procedure)
  }

  def clearProcedures(): Unit = {
    procedures.clear()
  }
}
