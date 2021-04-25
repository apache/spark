package org.apache.spark.sql.execution.datasources

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

import scala.collection.mutable.{Map => MutableMap}

object MergeUtils extends Logging{

  class AverageSize(var totalSize: Long, var numFiles: Int) {
    def getAverageSize: Long = {
      if (numFiles != 0) {
        totalSize / numFiles
      } else {
        0
      }
    }

    override def toString: String = "{totalSize: " + totalSize + ", numFiles: " + numFiles + "}"
  }

  def hasFile(path: Path, conf: Configuration): Boolean = {
    val fs = path.getFileSystem(conf)
    val fStatus = fs.listStatus(path)
    for (fStat <- fStatus) {
      if (fStat.isFile) {
        return true
      }
    }
    false
  }

  def getPathSize(inpFs: FileSystem, dirPath: Path): AverageSize = {
    val error = new AverageSize(-1, -1)
    try {
      val fStats = inpFs.listStatus(dirPath)
      var totalSz: Long = 0L
      var numFiles: Int = 0

      for (fStat <- fStats) {
        if (fStat.isDirectory()) {
          val avgSzDir = getPathSize(inpFs, fStat.getPath)
          if (avgSzDir.totalSize < 0) {
            return error
          }
          totalSz += avgSzDir.totalSize
          numFiles += avgSzDir.numFiles
        } else {
          if (!fStat.getPath.toString.endsWith("_SUCCESS")) {
            totalSz += fStat.getLen
            numFiles += 1
          }
        }
      }
      new AverageSize(totalSz, numFiles)
    } catch {
      case _: IOException => error
    }
  }

  // get all the dynamic partition path and it's file size
  def getTmpDynamicPartPathInfo(tmpPath: Path,
                                conf: Configuration): MutableMap[Path, AverageSize] = {
    val fs = tmpPath.getFileSystem(conf)
    val fStatus = fs.listStatus(tmpPath)
    val partPathInfo = MutableMap[Path, AverageSize]()

    for (fStat <- fStatus) {
      if (fStat.isDirectory) {
        logInfo("temp dynamic part path: " + fStat.getPath.toString)
        if (!hasFile(fStat.getPath, conf)) {
          partPathInfo ++= getTmpDynamicPartPathInfo(fStat.getPath, conf)
        } else {
          val avgSize = getPathSize(fs, fStat.getPath)
          logInfo("path2size (" + fStat.getPath.toString + " -> " + avgSize.totalSize + ")")
          partPathInfo += (fStat.getPath -> avgSize)
        }
      }
    }
    partPathInfo
  }

  def getExternalMergeTmpPath(tempPath: Path, hadoopConf: Configuration): Path = {
    val fs = tempPath.getFileSystem(hadoopConf)
    val dir = fs.makeQualified(tempPath)
    logInfo("Created temp merging dir = " + dir + " for path = " + tempPath)
    fs.delete(dir,true)
    try {
      if (!fs.mkdirs(dir)) {
        throw new IllegalStateException("Cannot create staging directory  '" + dir.toString + "'")
      }
      fs.deleteOnExit(dir)
    }
    catch {
      case e: IOException =>
        throw new RuntimeException(
          "Cannot create temp merging directory '" + dir.toString + "': " + e.getMessage, e)
    }
    return dir
  }

  def getRePartitionNum(path: Path, conf: Configuration, avgConditionSize:Long, avgOutputSize:Long): (Int,Long) = {
    var rePartitionNum = -1
    val inpFs = path.getFileSystem(conf)
    val totSize = getPathSize(inpFs, path)
    logInfo("static partition totalsize of path: " + path.toString + " is: "
      + totSize + "; numFiles is: " + totSize.numFiles)
    rePartitionNum = computeMergePartitionNum(totSize,avgConditionSize,avgOutputSize)
    (rePartitionNum, totSize.getAverageSize)
  }

  def computeMergePartitionNum(totSize: AverageSize, avgConditionSize:Long, avgOutputSize:Long): Int = {
    var partitionNum = -1
    if (totSize.numFiles <= 1) {
      partitionNum = -1
    } else {
      if (totSize.getAverageSize > avgConditionSize) {
        partitionNum = -1
      } else {
        partitionNum = Math.ceil(totSize.totalSize.toDouble / avgOutputSize).toInt
      }
    }
    partitionNum
  }

}
