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

import java.util.regex.PatternSyntaxException

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.catalyst.util.CollationFactory.Collation
import org.apache.spark.sql.types.{BooleanType, StringType}

/**
 * A command for `SHOW COLLATIONS`.
 *
 * The syntax of this command is:
 * {{{
 *    SHOW COLLATIONS (LIKE? pattern)?;
 * }}}
 */
case class ShowCollationsCommand(pattern: Option[String]) extends LeafRunnableCommand {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("name", StringType, nullable = false)(),
    AttributeReference("provider", StringType, nullable = false)(),
    AttributeReference("version", StringType, nullable = false)(),
    AttributeReference("binaryEquality", BooleanType, nullable = false)(),
    AttributeReference("binaryOrdering", BooleanType, nullable = false)(),
    AttributeReference("lowercaseEquality", BooleanType, nullable = false)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val allCollations = CollationFactory.fetchAllCollations().asScala.toSeq
    val filteredCollations = pattern.map(filterPattern(allCollations, _)).getOrElse(allCollations)
    filteredCollations.map(c => Row(
      c.collationName,
      c.provider,
      c.version,
      c.supportsBinaryEquality,
      c.supportsBinaryOrdering,
      c.supportsLowercaseEquality))
  }

  private def filterPattern(collations: Seq[Collation], pattern: String): Seq[Collation] = {
    val filteredCollations = mutable.SortedSet.empty[Collation]
    pattern.trim().split("\\|").foreach { subPattern =>
      try {
        val regex = ("(?i)" + subPattern.replaceAll("\\*", ".*")).r
        filteredCollations ++= collations.filter {
          collation => regex.pattern.matcher(collation.collationName).matches()
        }
      } catch {
        case _: PatternSyntaxException =>
      }
    }
    filteredCollations.toSeq
  }
}
