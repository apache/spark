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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.connector.catalog.CatalogManager.{
  SESSION_NAMESPACE,
  SYSTEM_CATALOG_NAME
}
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.LexicalThreadLocal

/**
 * A lexical registry of types for variables supplied by an analysis caller.
 *
 * Session-shaped names are stored as `system.session.<name>`. Other names
 * (SQL script locals, SQL function parameters) keep the supplied identity.
 */
private[sql] object SuppliedVariableRegistry
  extends LexicalThreadLocal[Seq[(Seq[String], DataType)]] {

  def create(variables: Seq[(Seq[String], DataType)]): Handle = {
    val entries = variables.map { case (nameParts, dataType) =>
      canonicalize(nameParts) -> dataType
    }
    createHandle(Some(entries))
  }

  def lookup(
      nameParts: Seq[String],
      resolver: (String, String) => Boolean): Option[DataType] = {
    get().flatMap(_.collectFirst {
      case (providedName, dataType) if sameName(providedName, nameParts, resolver) =>
        dataType
    })
  }

  def isSessionName(nameParts: Seq[String]): Boolean = nameParts.length == 1 || {
    if (nameParts.length == 2) {
      nameParts.head.equalsIgnoreCase(SESSION_NAMESPACE)
    } else if (nameParts.length == 3) {
      nameParts(0).equalsIgnoreCase(SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(SESSION_NAMESPACE)
    } else {
      false
    }
  }

  def sessionIdentity(name: String): Seq[String] = {
    Seq(SYSTEM_CATALOG_NAME, SESSION_NAMESPACE, name)
  }

  private def sameName(
      left: Seq[String],
      right: Seq[String],
      resolver: (String, String) => Boolean): Boolean = {
    left.length == right.length && left.zip(right).forall {
      case (a, b) => resolver(a, b)
    }
  }

  /** The identity a supplied name is stored and matched under. */
  def canonicalize(nameParts: Seq[String]): Seq[String] = {
    if (isSessionName(nameParts)) {
      sessionIdentity(nameParts.last)
    } else {
      nameParts
    }
  }
}
