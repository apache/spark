package org.apache.spark.graph.cypher.util

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try

object HadoopFSUtils {

  implicit class RichHadoopFileSystem(fileSystem: FileSystem) {

    def listDirectories(path: String): List[String] = {
      val p = new Path(path)
      Try(
        fileSystem
          .listStatus(p)
          .filter(_.isDirectory)
          .map(_.getPath.getName)
          .toList
      ).getOrElse(List.empty)
    }

  }

}
