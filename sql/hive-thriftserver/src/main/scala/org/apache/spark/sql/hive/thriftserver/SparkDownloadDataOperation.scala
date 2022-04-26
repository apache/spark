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

package org.apache.spark.sql.hive.thriftserver

import java.io.{File, IOException}
import java.security.PrivilegedExceptionAction
import java.util.{ArrayList => JArrayList, EnumSet => JEnumSet, Iterator => JIterator, List => JList, Map => JMap}
import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import scala.util.control.NonFatal

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hadoop.hive.shims.{Utils => HiveShimsUtils}
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.Operation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.Utils

private[hive] case class DownloadDataBlock(
    path: Option[Path] = None,
    offset: Option[Long] = None,
    dataSize: Long)

private[hive] class SparkDownloadDataOperation(
    val sqlContext: SQLContext,
    parentSession: HiveSession,
    tableName: String,
    query: String,
    format: String,
    options: JMap[String, String],
    runInBackground: Boolean)
  extends Operation(
    parentSession,
    Map.empty[String, String].asJava,
    OperationType.UNKNOWN_OPERATION,
    runInBackground) with SparkOperation with Logging {

  private val pathFilter = new PathFilter {
    override def accept(path: Path): Boolean =
      !path.getName.equals("_SUCCESS") && !path.getName.endsWith("crc")
  }

  // Please see CSVOptions for more details.
  private val defaultOptions = Map(
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
    "dateFormat" -> "yyyy-MM-dd",
    "delimiter" -> ",",
    "escape" -> "\"",
    "compression" -> "gzip",
    "header" -> "true",
    "maxRecordsPerFile" ->"0",
    "fetchBlockSize" -> "10485760")

  private val writeOptions =
    defaultOptions ++ Option(options).map(_.asScala).getOrElse(Map.empty[String, String]).toMap
  private val numFiles = writeOptions.get("numFiles").map(_.toInt)

  private val fetchSize = writeOptions("fetchBlockSize").toLong

  private val downloadQuery = s"Generating download files with arguments " +
    s"[${tableName}, ${query}, ${format}, ${writeOptions}]"

  private val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
  private val scratchDir = hadoopConf.get("hadoop.tmp.dir")
  private val pathPrefix = new Path(scratchDir + File.separator + "DownloadData" + File.separator +
    parentSession.getUserName + File.separator + parentSession.getSessionHandle.getSessionId)
  private val fs: FileSystem = pathPrefix.getFileSystem(hadoopConf)

  private var iter: JIterator[DownloadDataBlock] = _
  private var schemaStr: String = _
  private var totalDataSize: Long = 0

  override def close(): Unit = {
    logInfo(s"CLOSING $statementId")
    HiveThriftServer2.eventManager.onOperationClosed(statementId)
    cleanup(OperationState.CLOSED)
    sqlContext.sparkContext.clearJobGroup()
  }

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true)

    if (!runInBackground) {
      execute()
    } else {
      val sparkServiceUGI = HiveShimsUtils.getUGI()

      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  log.error("Error generating download file: ", e)
              }
            }
          }

          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              logError("Error generating download file as user : " +
                sparkServiceUGI.getShortUserName(), e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          parentSession.getSessionManager().submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError("Error generating download file in background", e)
          setState(OperationState.ERROR)
          throw new HiveSQLException(e)
      }
    }
  }

  private def execute(): Unit = {
    try {
      // Always use the latest class loader provided by executionHive's state.
      val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
      // Use parent session's SessionState in this operation because such SessionState
      // keeps some shared info per session e.g. authorization information.
      SessionState.setCurrentSessionState(parentSession.getSessionState)
      Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

      HiveThriftServer2.eventManager.onStatementStart(
        statementId,
        parentSession.getSessionHandle.getSessionId.toString,
        downloadQuery,
        statementId,
        parentSession.getUsername)

      assert(fetchSize >= 1L * 1024 * 1024 && fetchSize <= 20L * 1024 * 1024,
        s"fetchBlockSize(${fetchSize}) should be greater than 1M and less than 20M.")

      if (StringUtils.isNotEmpty(tableName) && StringUtils.isNotEmpty(query)) {
        throw new HiveSQLException("Both table name and query are specified.")
      }

      sqlContext.sparkContext.setJobGroup(statementId, downloadQuery)
      val resultPath = (Option(tableName), Option(query), Option(format), Option(options)) match {
        case (Some(t), None, _, _) =>
          writeData(sqlContext.table(t), new Path(pathPrefix, statementId))
        case (None, Some(q), _, _) =>
          writeData(sqlContext.sql(q), new Path(pathPrefix, statementId))
        case _ =>
          throw new HiveSQLException(s"Invalid arguments: ($tableName, $query, $format, $options).")
      }

      val dataSize = fs.getContentSummary(resultPath).getLength
      logInfo(s"Try to download ${dataSize} bytes data from thriftserver.")
      totalDataSize = dataSize

      val list: JList[DownloadDataBlock] = new JArrayList[DownloadDataBlock]()
      // Add total data size to first row.
      list.add(DownloadDataBlock(dataSize = dataSize))
      // and then add data.
      fs.listStatus(resultPath, pathFilter).map(_.getPath).foreach { path =>
        val dataLen = fs.getFileStatus(path).getLen
        // Cast to BigDecimal to avoid overflowing
        val fetchBatchs =
          BigDecimal(dataLen)./(BigDecimal(fetchSize)).setScale(0, RoundingMode.CEILING).longValue
        assert(fetchBatchs < Int.MaxValue, "The fetch batch too large.")

        (0 until fetchBatchs.toInt).foreach { i =>
          val fetchSizeInBatch = if (i == fetchBatchs - 1) dataLen - i * fetchSize else fetchSize
          list.add(DownloadDataBlock(
            path = Some(path), offset = Some(i * fetchSize), dataSize = fetchSizeInBatch))
        }

        list.add(DownloadDataBlock(path = Some(path), dataSize = -1))
      }

      iter = list.iterator()
      logInfo(s"Add ${list.size()} data blocks to be fetched.")

      HiveThriftServer2.eventManager.onStatementFinish(statementId)
      setState(OperationState.FINISHED)
    } catch {
      case NonFatal(e) =>
        logError("Error download file", e)
        setState(OperationState.ERROR)
        HiveThriftServer2.eventManager.onStatementError(
          statementId,
          ExceptionUtils.getRootCause(e).getMessage,
          Utils.exceptionString(e))
        val exception = new HiveSQLException(e)
        setOperationException(exception)
    }
  }

  private def writeData(df: DataFrame, path: Path): Path = {
    df.write
      .options(writeOptions)
      .format(Option(format).getOrElse("csv"))
      .save(path.toString)
    path
  }

  private var downloadedDataSize = 0L

  override def getNextRowSet(orientation: FetchOrientation, maxRowsL: Long): RowSet = {
    if (getStatus.getState ne OperationState.FINISHED) {
      throw getStatus.getOperationException
    }
    assertState(OperationState.FINISHED)
    validateFetchOrientation(orientation, JEnumSet.of(FetchOrientation.FETCH_NEXT))

    val rowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion, false)

    if (!iter.hasNext) {
      rowSet
    } else {
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val dataBlock = iter.next()
        val dataSize = dataBlock.dataSize
        dataBlock.path match {
          case Some(path) =>
            if (dataSize >= 0) {
              val buffer: Array[Byte] = new Array[Byte](dataSize.toInt)
              Utils.tryWithResource(fs.open(path)) { is =>
                is.seek(dataBlock.offset.get)
                is.readFully(buffer)
              }
              // data row
              rowSet.addRow(Array[AnyRef](path.getName, buffer, Long.box(dataSize)))
              downloadedDataSize = dataSize
            } else {
              // End of file row
              rowSet.addRow(Array[AnyRef](path.getName, null, Long.box(dataSize)))
            }
          case _ =>
            // Schema row and total data size row
            rowSet.addRow(Array[AnyRef](null, null, Long.box(dataSize)))
        }
        curRow += 1
      }
      rowSet
    }
  }

  override def getResultSetSchema: TableSchema = {
    new TableSchema()
      .addPrimitiveColumn("FILE_NAME", Type.STRING_TYPE, "The file name to be transferred.")
      .addPrimitiveColumn("DATA", Type.BINARY_TYPE, "The data to be transferred.")
      .addPrimitiveColumn("SIZE", Type.BIGINT_TYPE, "The size to be transferred in this fetch.")
  }

  override def cancel(): Unit = {
    if (statementId != null) {
      HiveThriftServer2.eventManager.onStatementCanceled(statementId)
    }
    cleanup(OperationState.CANCELED)
  }

  private def cleanup(state: OperationState): Unit = {
    setState(state)
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle()
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
    if (statementId != null) {
      sqlContext.sparkContext.cancelJobGroup(statementId)
    }

    // Delete temp files
    try {
      fs.delete(pathPrefix, true)
    } catch {
      case e: IOException =>
        log.warn("Failed to remove download temp files.", e)
    }
  }
}
