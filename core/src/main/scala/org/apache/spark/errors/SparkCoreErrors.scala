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

package org.apache.spark.errors

import java.io.{File, IOException}
import java.util.concurrent.TimeoutException

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException, SparkUnsupportedOperationException, TaskNotSerializableException}
import org.apache.spark.internal.config.IO_COMPRESSION_CODEC
import org.apache.spark.io.CompressionCodec.FALLBACK_COMPRESSION_CODEC
import org.apache.spark.memory.SparkOutOfMemoryError
import org.apache.spark.scheduler.{BarrierJobRunWithDynamicAllocationException, BarrierJobSlotsNumberCheckFailed, BarrierJobUnsupportedRDDChainException}
import org.apache.spark.shuffle.{FetchFailedException, ShuffleManager}
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockNotFoundException, BlockSavedOnDecommissionedBlockManagerException, RDDBlockId, UnrecognizedBlockId}

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 */
private[spark] object SparkCoreErrors {
  def unexpectedPy4JServerError(other: Object): Throwable = {
    new SparkRuntimeException(
      errorClass = "_LEGACY_ERROR_TEMP_3000",
      messageParameters = Map("class" -> s"${other.getClass}")
    )
  }

  def eofExceptionWhileReadPortNumberError(
      daemonModule: String,
      daemonExitValue: Option[Int] = None): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3001",
      messageParameters = Map(
        "daemonModule" -> daemonModule,
        "additionalMessage" ->
          daemonExitValue.map(v => s" and terminated with code: $v.").getOrElse("")
      ), cause = null
    )
  }

  def unsupportedDataTypeError(other: Any): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3002",
      messageParameters = Map("other" -> s"$other"),
      cause = null
    )
  }

  def rddBlockNotFoundError(blockId: BlockId, id: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3003",
      messageParameters = Map("blockId" -> s"$blockId", "id" -> s"$id"),
      cause = null
    )
  }

  def blockHaveBeenRemovedError(string: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3004",
      messageParameters = Map("string" -> string),
      cause = null
    )
  }

  def histogramOnEmptyRDDOrContainingInfinityOrNaNError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3005")
  }

  def emptyRDDError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3006")
  }

  def pathNotSupportedError(path: String): Throwable = {
    new IOException(s"Path: ${path} is a directory, which is not supported by the " +
      "record reader when `mapreduce.input.fileinputformat.input.dir.recursive` is false.")
  }

  def checkpointRDDBlockIdNotFoundError(rddBlockId: RDDBlockId): Throwable = {
    new SparkException(
      errorClass = "CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND",
      messageParameters = Map("rddBlockId" -> s"$rddBlockId"),
      cause = null
    )
  }

  def endOfStreamError(): Throwable = {
    new java.util.NoSuchElementException("End of stream")
  }

  def cannotUseMapSideCombiningWithArrayKeyError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3008", messageParameters = Map.empty, cause = null
    )
  }

  def hashPartitionerCannotPartitionArrayKeyError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3009", messageParameters = Map.empty, cause = null
    )
  }

  def reduceByKeyLocallyNotSupportArrayKeysError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3010", messageParameters = Map.empty, cause = null
    )
  }

  def rddLacksSparkContextError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3011", messageParameters = Map.empty, cause = null
    )
  }

  def cannotChangeStorageLevelError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3012")
  }

  def canOnlyZipRDDsWithSamePartitionSizeError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3013", messageParameters = Map.empty, cause = null
    )
  }

  def emptyCollectionError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3014")
  }

  def countByValueApproxNotSupportArraysError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3015", messageParameters = Map.empty, cause = null
    )
  }

  def checkpointDirectoryHasNotBeenSetInSparkContextError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3016", messageParameters = Map.empty, cause = null
    )
  }

  def invalidCheckpointFileError(path: Path): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3017",
      messageParameters = Map("path" -> s"$path"),
      cause = null
    )
  }

  def failToCreateCheckpointPathError(checkpointDirPath: Path): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3018",
      messageParameters = Map("checkpointDirPath" -> s"$checkpointDirPath"),
      cause = null
    )
  }

  def checkpointRDDHasDifferentNumberOfPartitionsFromOriginalRDDError(
      originalRDDId: Int,
      originalRDDLength: Int,
      newRDDId: Int,
      newRDDLength: Int): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3019",
      messageParameters = Map(
        "originalRDDId" -> s"$originalRDDId",
        "originalRDDLength" -> s"$originalRDDLength",
        "newRDDId" -> s"$newRDDId",
        "newRDDLength" -> s"$newRDDLength"
      ),
      cause = null
    )
  }

  def checkpointFailedToSaveError(task: Int, path: Path): Throwable = {
    new IOException("Checkpoint failed: failed to save output of task: " +
      s"$task and final output path does not exist: $path")
  }

  def mustSpecifyCheckpointDirError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3020", messageParameters = Map.empty, cause = null
    )
  }

  def askStandaloneSchedulerToShutDownExecutorsError(e: Exception): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3021", messageParameters = Map.empty, cause = e
    )
  }

  def stopStandaloneSchedulerDriverEndpointError(e: Exception): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3022", messageParameters = Map.empty, cause = e
    )
  }

  def noExecutorIdleError(id: String): Throwable = {
    new NoSuchElementException(id)
  }

  def sparkJobCancelled(jobId: Int, reason: String, e: Exception): SparkException = {
    new SparkException(
      errorClass = "SPARK_JOB_CANCELLED",
      messageParameters = Map("jobId" -> jobId.toString, "reason" -> reason),
      cause = e
    )
  }

  def sparkJobCancelledAsPartOfJobGroupError(jobId: Int, jobGroupId: String): SparkException = {
    sparkJobCancelled(jobId, s"part of cancelled job group $jobGroupId", null)
  }

  def barrierStageWithRDDChainPatternError(): Throwable = {
    new BarrierJobUnsupportedRDDChainException
  }

  def barrierStageWithDynamicAllocationError(): Throwable = {
    new BarrierJobRunWithDynamicAllocationException
  }

  def numPartitionsGreaterThanMaxNumConcurrentTasksError(
      numPartitions: Int,
      maxNumConcurrentTasks: Int): Throwable = {
    new BarrierJobSlotsNumberCheckFailed(numPartitions, maxNumConcurrentTasks)
  }

  def cannotRunSubmitMapStageOnZeroPartitionRDDError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3023", messageParameters = Map.empty, cause = null
    )
  }

  def accessNonExistentAccumulatorError(id: Long): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3024", messageParameters = Map("id" -> s"$id"), cause = null
    )
  }

  def sendResubmittedTaskStatusForShuffleMapStagesOnlyError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3025", messageParameters = Map.empty, cause = null
    )
  }

  def nonEmptyEventQueueAfterTimeoutError(timeoutMillis: Long): Throwable = {
    new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
  }

  def durationCalledOnUnfinishedTaskError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3026")
  }

  def unrecognizedSchedulerModePropertyError(
      schedulerModeProperty: String,
      schedulingModeConf: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3027",
      messageParameters = Map(
        "schedulerModeProperty" -> schedulerModeProperty,
        "schedulingModeConf" -> schedulingModeConf
      ),
      cause = null
    )
  }

  def sparkError(errorMsg: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3028",
      messageParameters = Map("errorMsg" -> errorMsg),
      cause = null
    )
  }

  def clusterSchedulerError(message: String): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3029",
      messageParameters = Map("message" -> message),
      cause = null
    )
  }

  def failToSerializeTaskError(e: Throwable): Throwable = {
    new TaskNotSerializableException(e)
  }

  def unrecognizedBlockIdError(name: String): Throwable = {
    new UnrecognizedBlockId(name)
  }

  def taskHasNotLockedBlockError(currentTaskAttemptId: Long, blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3030",
      messageParameters = Map(
        "currentTaskAttemptId" -> s"$currentTaskAttemptId",
        "blockId" -> s"$blockId"
      ),
      cause = null
    )
  }

  def blockDoesNotExistError(blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3031",
      messageParameters = Map("blockId" -> s"$blockId"),
      cause = null
    )
  }

  def cannotSaveBlockOnDecommissionedExecutorError(blockId: BlockId): Throwable = {
    new BlockSavedOnDecommissionedBlockManagerException(blockId)
  }

  def waitingForReplicationToFinishError(e: Throwable): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3032", messageParameters = Map.empty, cause = e
    )
  }

  def unableToRegisterWithExternalShuffleServerError(e: Throwable): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3033",
      messageParameters = Map("message" -> e.getMessage),
      cause = e
    )
  }

  def waitingForAsyncReregistrationError(e: Throwable): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3034", messageParameters = Map.empty, cause = e
    )
  }

  def unexpectedShuffleBlockWithUnsupportedResolverError(
      shuffleManager: ShuffleManager,
      blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3035",
      messageParameters = Map(
        "blockId" -> s"$blockId",
        "shuffleBlockResolver" -> s"${shuffleManager.shuffleBlockResolver}"
      ),
      cause = null
    )
  }

  def failToStoreBlockOnBlockManagerError(
      blockManagerId: BlockManagerId,
      blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3036",
      messageParameters = Map(
        "blockId" -> s"$blockId",
        "blockManagerId" -> s"$blockManagerId"
      ),
      cause = null
    )
  }

  def readLockedBlockNotFoundError(blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3037",
      messageParameters = Map(
        "blockId" -> s"$blockId"
      ),
      cause = null
    )
  }

  def failToGetBlockWithLockError(blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3038",
      messageParameters = Map(
        "blockId" -> s"$blockId"
      ),
      cause = null
    )
  }

  def blockNotFoundError(blockId: BlockId): Throwable = {
    new BlockNotFoundException(blockId.toString)
  }

  def interruptedError(): Throwable = {
    new InterruptedException()
  }

  def blockStatusQueryReturnedNullError(blockId: BlockId): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3039",
      messageParameters = Map("blockId" -> s"$blockId"),
      cause = null
    )
  }

  def unexpectedBlockManagerMasterEndpointResultError(): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3040", messageParameters = Map.empty, cause = null
    )
  }

  def failToCreateDirectoryError(path: String, maxAttempts: Int): Throwable = {
    new IOException(
      s"Failed to create directory ${path} with permission 770 after $maxAttempts attempts!")
  }

  def unsupportedOperationError(): Throwable = {
    new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3041")
  }

  def noSuchElementError(): Throwable = {
    new NoSuchElementException()
  }

  def fetchFailedError(
      bmAddress: BlockManagerId,
      shuffleId: Int,
      mapId: Long,
      mapIndex: Int,
      reduceId: Int,
      message: String,
      cause: Throwable = null): Throwable = {
    new FetchFailedException(bmAddress, shuffleId, mapId, mapIndex, reduceId, message, cause)
  }

  def failToGetNonShuffleBlockError(blockId: BlockId, e: Throwable): Throwable = {
    new SparkException(
      errorClass = "_LEGACY_ERROR_TEMP_3042",
      messageParameters = Map("blockId" -> s"$blockId"),
      cause = e
    )
  }

  def graphiteSinkInvalidProtocolError(invalidProtocol: String): Throwable = {
    new SparkException(
      errorClass = "GRAPHITE_SINK_INVALID_PROTOCOL",
      messageParameters = Map("protocol" -> invalidProtocol),
      cause = null)
  }

  def graphiteSinkPropertyMissingError(missingProperty: String): Throwable = {
    new SparkException(
      errorClass = "GRAPHITE_SINK_PROPERTY_MISSING",
      messageParameters = Map("property" -> missingProperty),
      cause = null)
  }

  def outOfMemoryError(requestedBytes: Long, receivedBytes: Long): OutOfMemoryError = {
    new SparkOutOfMemoryError(
      "UNABLE_TO_ACQUIRE_MEMORY",
      Map(
        "requestedBytes" -> requestedBytes.toString,
        "receivedBytes" -> receivedBytes.toString).asJava)
  }

  def failedRenameTempFileError(srcFile: File, dstFile: File): Throwable = {
    new SparkException(
      errorClass = "FAILED_RENAME_TEMP_FILE",
      messageParameters = Map(
        "srcPath" -> srcFile.toString,
        "dstPath" -> dstFile.toString),
      cause = null)
  }

  def addLocalDirectoryError(path: Path): Throwable = {
    new SparkException(
      errorClass = "UNSUPPORTED_ADD_FILE.LOCAL_DIRECTORY",
       messageParameters = Map("path" -> path.toString),
      cause = null)
  }

  def addDirectoryError(path: Path): Throwable = {
    new SparkException(
      errorClass = "UNSUPPORTED_ADD_FILE.DIRECTORY",
      messageParameters = Map("path" -> path.toString),
      cause = null)
  }

  def codecNotAvailableError(codecName: String): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "CODEC_NOT_AVAILABLE.WITH_CONF_SUGGESTION",
      messageParameters = Map(
        "codecName" -> codecName,
        "configKey" -> toConf(IO_COMPRESSION_CODEC.key),
        "configVal" -> toConfVal(FALLBACK_COMPRESSION_CODEC)))
  }

  def tooManyArrayElementsError(numElements: Long, maxRoundedArrayLength: Int): Throwable = {
    new SparkIllegalArgumentException(
      errorClass = "COLLECTION_SIZE_LIMIT_EXCEEDED.INITIALIZE",
      messageParameters = Map(
        "numberOfElements" -> numElements.toString,
        "maxRoundedArrayLength" -> maxRoundedArrayLength.toString)
    )
  }

  private def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

  def toConf(conf: String): String = {
    quoteByDefault(conf)
  }

  def toConfVal(conf: String): String = {
    quoteByDefault(conf)
  }
}
