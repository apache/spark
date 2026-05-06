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

import java.util.Locale

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.PathElement
import org.apache.spark.sql.internal.SQLConf

/**
 * Command for SET PATH = pathElement (, pathElement)*
 * Expands shortcuts at run time, validates no duplicates, and sets the internal session path.
 *
 * The [[PathElement]] AST and its expansion live in catalyst so that the same grammar can be
 * reused to parse the [[SQLConf.DEFAULT_PATH]] conf value.
 */
case class SetPathCommand(elements: Seq[PathElement]) extends LeafRunnableCommand {

  override def output: Seq[Attribute] = Seq.empty

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (!sparkSession.sessionState.conf.pathEnabled) {
      throw new AnalysisException(
        errorClass = "UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED",
        messageParameters = Map("config" -> SQLConf.PATH_ENABLED.key))
    }
    val conf = sparkSession.sessionState.conf
    val catalogManager = sparkSession.sessionState.catalogManager
    val currentCatalog = catalogManager.currentCatalog.name
    val currentNamespace = catalogManager.currentNamespace.toSeq
    val caseSensitive = conf.caseSensitiveAnalysis

    val expanded = PathElement.expand(elements, conf, catalogManager)
    val seen = new scala.collection.mutable.HashSet[Seq[String]]
    expanded.foreach { entry =>
      val concrete = entry.resolve(currentCatalog, currentNamespace)
      def normalize(s: String): String = if (caseSensitive) s else s.toLowerCase(Locale.ROOT)
      val key = concrete.map(normalize)
      if (!seen.add(key)) {
        throw new AnalysisException(
          errorClass = "DUPLICATE_SQL_PATH_ENTRY",
          messageParameters = Map("pathEntry" ->
            concrete.map(p => if (p.contains(".")) s"`$p`" else p).mkString(".")))
      }
    }

    if (expanded.isEmpty) {
      catalogManager.clearSessionPath()
    } else {
      catalogManager.setSessionPath(expanded)
    }
    Seq.empty
  }
}
