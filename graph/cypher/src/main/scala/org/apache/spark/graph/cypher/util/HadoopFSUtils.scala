package org.apache.spark.graph.cypher.util

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.Try

object HadoopFSUtils {

  implicit class RichHadoopFileSystem(fileSystem: FileSystem) {

    def listDirectories(path: String): Seq[String] = {
      val p = new Path(path)
      fileSystem
        .listStatus(p)
        .collect { case item if item.isDirectory =>
          item.getPath.getName
        }
        .toSeq
    }

  }

}
