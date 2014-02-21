package org.apache.spark

object Version {
  def getRevision(): String = {
    getStaticFile("/revision.txt").trim()
  }

  def getStaticFile(fileName: String): String = {
    val stream = getClass.getResourceAsStream(fileName)
    scala.io.Source.fromInputStream(stream).mkString
  }
}
