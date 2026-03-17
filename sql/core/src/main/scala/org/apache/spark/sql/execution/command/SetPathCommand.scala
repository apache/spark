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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

/**
 * Path element for SET PATH: either a well-known shortcut or a schema (optionally qualified).
 * For SchemaInPath(parts), qualification with current catalog or SYSTEM is done at run time.
 */
sealed trait PathElement

object PathElement {
  case object DefaultPath extends PathElement
  case object SystemPath extends PathElement
  case object CurrentSchema extends PathElement
  case object PathRef extends PathElement
  /** Schema name parts (1 = unqualified namespace, 2+ = catalog.namespace...). Qualified at run. */
  case class SchemaInPath(parts: Seq[String]) extends PathElement
}

/**
 * Command for SET PATH = pathElement (, pathElement)*
 * Expands shortcuts at run time, validates no duplicates, and sets the internal session path.
 */
case class SetPathCommand(elements: Seq[PathElement]) extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq.empty

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!sparkSession.sessionState.conf.pathEnabled) {
      // Feature disabled: no-op (or could throw; plan says reject or no-op)
      return Seq.empty
    }
    val conf = sparkSession.sessionState.conf
    val catalogManager = sparkSession.sessionState.catalogManager
    val currentCatalog = catalogManager.currentCatalog.name
    val currentNamespace = catalogManager.currentNamespace.toSeq

    val expanded = expandPathElements(elements, conf, currentCatalog, currentNamespace)
    val seen = new scala.collection.mutable.HashSet[(String, String)]
    val deduped = expanded.flatMap { entry =>
      val key = (entry.head.toLowerCase(java.util.Locale.ROOT),
        entry.lift(1).getOrElse("").toLowerCase(java.util.Locale.ROOT))
      if (seen.contains(key)) {
        throw new AnalysisException(
          errorClass = "DUPLICATE_PATH_ENTRY",
          messageParameters = Map("pathEntry" -> entry.mkString(".")))
      }
      seen.add(key)
      Some(entry)
    }

    if (deduped.isEmpty) {
      conf.unsetConf(SQLConf.SESSION_PATH)
    } else {
      conf.setConfString(SQLConf.SESSION_PATH.key, SQLConf.formatSessionPath(deduped))
    }
    Seq.empty
  }

  private def expandPathElements(
      elements: Seq[PathElement],
      conf: SQLConf,
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] = {
    val systemCatalog = CatalogManager.SYSTEM_CATALOG_NAME
    val builtin = CatalogManager.BUILTIN_NAMESPACE
    val session = CatalogManager.SESSION_NAMESPACE

    elements.flatMap {
      case PathElement.DefaultPath =>
        defaultPathEntries(conf, systemCatalog, builtin, session, currentCatalog, currentNamespace)
      case PathElement.SystemPath =>
        Seq(Seq(systemCatalog, builtin), Seq(systemCatalog, session))
      case PathElement.CurrentSchema =>
        if (currentNamespace.isEmpty) Seq(Seq(currentCatalog))
        else Seq(currentCatalog +: currentNamespace)
      case PathElement.PathRef =>
        conf.sessionPath match {
          case Some(s) => SQLConf.parseSessionPath(s)
          case None =>
            defaultPathEntries(
              conf, systemCatalog, builtin, session, currentCatalog, currentNamespace)
        }
      case PathElement.SchemaInPath(parts) =>
        qualifySchemaParts(parts, systemCatalog, currentCatalog)
    }
  }

  /** Qualify schema parts at SET time: well-known -> SYSTEM; else current catalog + namespace. */
  private def qualifySchemaParts(
      parts: Seq[String],
      systemCatalog: String,
      currentCatalog: String): Seq[Seq[String]] = {
    val wellKnown = Set(
      CatalogManager.BUILTIN_NAMESPACE.toLowerCase(java.util.Locale.ROOT),
      CatalogManager.SESSION_NAMESPACE.toLowerCase(java.util.Locale.ROOT))
    if (parts.isEmpty) return Seq.empty
    if (parts.size == 1) {
      val ns = parts.head
      if (wellKnown.contains(ns.toLowerCase(java.util.Locale.ROOT))) {
        Seq(Seq(systemCatalog, ns))
      } else {
        Seq(Seq(currentCatalog, ns))
      }
    } else {
      Seq(Seq(parts.head, parts(1)))
    }
  }

  private def defaultPathEntries(
      conf: SQLConf,
      systemCatalog: String,
      builtin: String,
      session: String,
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] = {
    val order = conf.sessionFunctionResolutionOrder
    val sessionNs = Seq(systemCatalog, session)
    val builtinNs = Seq(systemCatalog, builtin)
    val currentNs =
      if (currentNamespace.isEmpty) Seq(currentCatalog) else currentCatalog +: currentNamespace
    order match {
      case "first" => Seq(sessionNs, builtinNs, currentNs)
      case "last" => Seq(builtinNs, currentNs, sessionNs)
      case _ => Seq(builtinNs, sessionNs, currentNs)
    }
  }
}
