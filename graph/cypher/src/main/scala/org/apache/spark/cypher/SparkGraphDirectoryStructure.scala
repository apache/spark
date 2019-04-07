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
 *
 */

package org.apache.spark.cypher

import org.apache.hadoop.fs.Path

object SparkGraphDirectoryStructure {

  import org.apache.spark.cypher.conversions.StringEncodingUtilities._

  private implicit class StringPath(val path: String) extends AnyVal {
    def /(segment: String): String = s"$path$pathSeparator$segment"
  }

  val pathSeparator: String = Path.SEPARATOR

  val nodeTablesDirectoryName = "nodes"

  val relationshipTablesDirectoryName = "relationships"

  // Because an empty path does not work, we need a special directory name for nodes without labels.
  val noLabelNodeDirectoryName: String = "__NO_LABEL__"

  def nodeTableDirectoryName(labels: Set[String]): String = concatDirectoryNames(labels.toSeq.sorted)

  def relKeyTableDirectoryName(relKey: String): String = relKey.encodeSpecialCharacters

  def concatDirectoryNames(seq: Seq[String]): String = {
    if (seq.isEmpty) {
      noLabelNodeDirectoryName
    } else {
      // TODO: Find more elegant solution for encoding underline characters
      seq.map(_.replace("_", "--UNDERLINE--")).mkString("_").encodeSpecialCharacters
    }
  }

  implicit class ComboDirectory(val directoryName: String) extends AnyVal {

    def toLabelCombo: Set[String] = {
      directoryName match {
        case `noLabelNodeDirectoryName` => Set.empty
        case encodedLabelString =>
          val encodedLabels = encodedLabelString.split('_').toSet
          // TODO: Find more elegant solution for decoding underline characters
          encodedLabels.map(_.decodeSpecialCharacters.replace("--UNDERLINE--", "_"))
      }
    }

    def toRelationshipType: String = directoryName.decodeSpecialCharacters

  }

}

case class SparkGraphDirectoryStructure(rootPath: String) {

  import SparkGraphDirectoryStructure._

  def pathToNodeTable(labels: Set[String]): String = pathToNodeDirectory / nodeTableDirectoryName(labels)

  def pathToRelationshipTable(relKey: String): String = pathToRelationshipDirectory / relKeyTableDirectoryName(relKey)

  def pathToNodeDirectory: String = rootPath / nodeTablesDirectoryName

  def pathToRelationshipDirectory: String = rootPath / relationshipTablesDirectoryName

}
