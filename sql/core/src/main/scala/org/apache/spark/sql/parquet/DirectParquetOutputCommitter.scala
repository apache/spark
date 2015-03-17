package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter

import parquet.Log
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.{ParquetFileReader, ParquetFileWriter, ParquetOutputCommitter}

private[parquet] class DirectParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {
  val LOG = Log.getLog(classOf[ParquetOutputCommitter])

  override def getWorkPath(): Path = outputPath
  override def abortTask(taskContext: TaskAttemptContext): Unit = {}
  override def commitTask(taskContext: TaskAttemptContext): Unit = {}
  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = true
  override def setupJob(jobContext: JobContext): Unit = {}
  override def setupTask(taskContext: TaskAttemptContext): Unit = {}

  override def commitJob(jobContext: JobContext) {
    try {
      val configuration = ContextUtil.getConfiguration(jobContext)
      val fileSystem = outputPath.getFileSystem(configuration)
      val outputStatus = fileSystem.getFileStatus(outputPath)
      val footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus)
      try {
        ParquetFileWriter.writeMetadataFile(configuration, outputPath, footers)
        if (configuration.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
          val successPath = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)
          fileSystem.create(successPath).close()
        }
      } catch {
        case e: Exception => {
          LOG.warn("could not write summary file for " + outputPath, e)
          val metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE)
          if (fileSystem.exists(metadataPath)) {
            fileSystem.delete(metadataPath, true)
          }
        }
      }
    } catch {
      case e: Exception => LOG.warn("could not write summary file for " + outputPath, e)
    }
  }

}

