package org.apache.spark.cypher.util

import org.apache.hadoop.fs.{FileSystem, Path}

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
