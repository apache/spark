package org.apache.spark.graph.cypher

import org.apache.hadoop.fs.Path

object SparkGraphDirectoryStructure {

  import org.apache.spark.graph.cypher.conversions.StringEncodingUtilities._

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
