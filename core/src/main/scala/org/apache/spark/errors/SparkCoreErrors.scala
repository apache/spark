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

import java.io.{EOFException, File, FileNotFoundException, IOException}
import java.net.ConnectException
import java.util.NoSuchElementException
import java.util.concurrent.TimeoutException
import javax.servlet.ServletException

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkException, SparkUserAppException, TaskContext, TaskKilledException, TaskNotSerializableException}
import org.apache.spark.deploy.rest.{SubmitRestConnectionException, SubmitRestMissingFieldException, SubmitRestProtocolException}
import org.apache.spark.internal.config.History
import org.apache.spark.scheduler.{BarrierJobRunWithDynamicAllocationException, BarrierJobSlotsNumberCheckFailed, BarrierJobUnsupportedRDDChainException}
import org.apache.spark.scheduler.HaltReplayException
import org.apache.spark.shuffle.{FetchFailedException, ShuffleManager}
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockNotFoundException, BlockSavedOnDecommissionedBlockManagerException, RDDBlockId, UnrecognizedBlockId}

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 */
object SparkCoreErrors {
  def unexpectedPy4JServerError(other: Object): Throwable = {
    new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }

  def eofExceptionWhileReadPortNumberError(
      daemonModule: String,
      daemonExitValue: Option[Int] = null): Throwable = {
    val msg = s"EOFException occurred while reading the port number from $daemonModule's" +
      s" stdout" + daemonExitValue.map(v => s" and terminated with code: $v.").getOrElse("")
    new SparkException(msg)
  }

  def unsupportedDataTypeError(other: Any): Throwable = {
    new SparkException(s"Data of type $other is not supported")
  }

  def failToGetIntoAcceptableClusterStateError(e: TimeoutException): Throwable = {
    new RuntimeException("Failed to get into acceptable cluster state after 2 min.", e)
  }

  def serverSocketFailedToBindJavaSideError(): Throwable = {
    new SparkException("ServerSocket failed to bind to Java side.")
  }

  def taskKilledError(context: TaskContext): Throwable = {
    new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))
  }

  def pythonWorkerExitedError(eof: EOFException, error: String = null): Throwable = {
    val msg = "Python worker exited unexpectedly (crashed)"
    if(error == null) {
      new SparkException(msg, eof)
    } else {
      new SparkException(msg + s": $error", eof)
    }
  }

  def sparkRBackendInitializationError(backendTimeout: Int): Throwable = {
    new SparkException(s"SparkR backend did not initialize in $backendTimeout seconds")
  }

  def sparkUserAppError(returnCode: Int): Throwable = {
    SparkUserAppException(returnCode)
  }

  def keytabFileNotExistError(keytabFilename: String): Throwable = {
    new SparkException(s"Keytab file: ${keytabFilename} does not exist")
  }

  def failToCreateParentsError(path: Path): Throwable = {
    new IOException(s"Failed to create parents of $path")
  }

  def failToLoadIvySettingError(settingsFile: String, e: Throwable): Throwable = {
    new SparkException(s"Failed when loading Ivy settings from $settingsFile", e)
  }

  def invalidSparkConfigPairError(pair: String): Throwable = {
    new SparkException(s"Spark config without '=': $pair")
  }

  def multipleExternalSparkSubmitOperationsRegisteredError(x: Int, master: String): Throwable = {
    new SparkException(s"Multiple($x) external SparkSubmitOperations " +
      s"clients registered for master url ${master}.")
  }

  def securityError(): Throwable = {
    new SecurityException()
  }

  def failToPrepareResourceFileError(compShortName: String, e: Throwable): Throwable = {
    new SparkException(s"Exception threw while preparing resource file for $compShortName", e)
  }

  def noApplicationWithApplicationIdError(appId: String, attemptId: Option[String]): Throwable = {
    new NoSuchElementException(s"no application with application Id '$appId'" +
      attemptId.map { id => s" attemptId '$id'" }.getOrElse(" and no attempt Id"))
  }

  def filterOnlyWorksForHTTPError(): Throwable = {
    new ServletException("This filter only works for HTTP/HTTPS")
  }

  def logFileAlreadyExistsError(dest: Path): Throwable = {
    new IOException(s"Target log file already exists ($dest)")
  }

  def logDirectoryAlreadyExistsError(logDirForAppPath: Path): Throwable = {
    new IOException(s"Target log directory already exists ($logDirForAppPath)")
  }

  def logDirNotExistError(logDir: String, f: FileNotFoundException): Throwable = {
    var msg = s"Log directory specified does not exist: $logDir"
    if (logDir == History.DEFAULT_LOG_DIR) {
      msg += " Did you configure the correct one through spark.history.fs.logDirectory?"
    }
    new FileNotFoundException(msg).initCause(f)
  }

  def noSuchElementError(s: String): Throwable = {
    new NoSuchElementException(s)
  }

  def logsNotFoundForAppError(appId: String): Throwable = {
    new SparkException(s"Logs for $appId not found.")
  }

  def cannotFindAttemptOfAppIdError(attemptId: Option[String], appId: String): Throwable = {
    new NoSuchElementException(s"Cannot find attempt $attemptId of $appId.")
  }

  def haltReplayError(): Throwable = {
    new HaltReplayException()
  }

  def keyMustBePositiveError(s: String): Throwable = {
    new SparkException(s"${s} must be positive")
  }

  def unexpectedStateUpdateForDriverError(driverId: String, state: String): Throwable = {
    new Exception(s"Received unexpected state update for driver $driverId: $state")
  }

  def unableToConnectToServerError(e: Throwable): Throwable = {
    new SubmitRestConnectionException("Unable to connect to server", e)
  }

  def connectServerError(e: ConnectException): Throwable = {
    new SubmitRestConnectionException("Connect Exception when connect to server", e)
  }

  def serverRespondedWithExceptionError(s: Option[String]): Throwable = {
    new SubmitRestProtocolException(s"Server responded with exception:\n${s}")
  }

  def serverReturnedEmptyBodyError(): Throwable = {
    new SubmitRestProtocolException("Server returned empty body")
  }

  def messageReceivedFromServerWasNotAResponseError(response: String): Throwable = {
    new SubmitRestProtocolException(
      s"Message received from server was not a response:\n$response")
  }

  def malformedResponseReceivedFromServerError(malformed: Throwable): Throwable = {
    new SubmitRestProtocolException("Malformed response received from server", malformed)
  }

  def noResponseFromServerError(timeout: Throwable): Throwable = {
    new SubmitRestConnectionException("No response from server", timeout)
  }

  def waitingForResponseError(t: Throwable): Throwable = {
    new SparkException("Exception while waiting for response", t)
  }

  def applicationJarMissingError(): Throwable = {
    new SubmitRestMissingFieldException("Application jar is missing.")
  }

  def mainClassMissingError(): Throwable = {
    new SubmitRestMissingFieldException("Main class is missing.")
  }

  def failToValidateMessageError(messageType: String, e: Exception): Throwable = {
    new SubmitRestProtocolException(s"Validation of message $messageType failed!", e)
  }

  def missingActionFieldError(messageType: String): Throwable = {
    new SubmitRestMissingFieldException(s"The action field is missing in $messageType")
  }

  def missingFieldInMessageError(messageType: String, name: String): Throwable = {
    new SubmitRestMissingFieldException(s"'$name' is missing in message $messageType.")
  }

  def actionFieldNotFoundInJsonError(json: String): Throwable = {
    new SubmitRestMissingFieldException(s"Action field not found in JSON:\n$json")
  }

  def unexpectedValueForPropertyError(key: String, valueType: String, value: String): Throwable = {
    new SubmitRestProtocolException(
      s"Property '$key' expected $valueType value: actual was '$value'.")
  }

  def cannotGetMasterKerberosPrincipalRenewerError(): Throwable = {
    new SparkException("Can't get Master Kerberos principal for use as renewer.")
  }

  def failToCreateDirectoryError(driverDir: File): Throwable = {
    new IOException(s"Failed to create directory $driverDir")
  }

  def cannotFindJarInDriverDirectoryError(jarFileName: String, driverDir: File): Throwable = {
    new IOException(
      s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
  }

  def failToListFilesInAppDirsError(appDirs: Array[File]): Throwable = {
    new IOException(s"ERROR: Failed to list files in $appDirs")
  }

  def cannotCreateSubfolderInLocalRootDirsError(localRootDirs: String): Throwable = {
    new IOException(s"No subfolder can be created in ${localRootDirs}.")
  }

  def requestMustSpecifyApplicationOrDriverError(): Throwable = {
    new Exception("Request must specify either application or driver identifiers")
  }

  def rddBlockNotFoundError(blockId: BlockId, id: Int): Throwable = {
    new Exception(s"Could not compute split, block $blockId of RDD $id not found")
  }

  def blockHaveBeenRemovedError(string: String): Throwable = {
    new SparkException(s"Attempted to use $string after its blocks have been removed!")
  }

  def histogramOnEmptyRDDOrContainingInfinityOrNaNError(): Throwable = {
    new UnsupportedOperationException(
      "Histogram on either an empty RDD or RDD containing +/-infinity or NaN")
  }

  def emptyRDDError(): Throwable = {
    new UnsupportedOperationException("empty RDD")
  }

  def pathNotSupportedError(path: String): Throwable = {
    new IOException(s"Path: ${path} is a directory, which is not supported by the " +
      "record reader when `mapreduce.input.fileinputformat.input.dir.recursive` is false.")
  }

  def checkpointRDDBlockIdNotFoundError(rddBlockId: RDDBlockId): Throwable = {
    new SparkException(
      s"""
         |Checkpoint block $rddBlockId not found! Either the executor
         |that originally checkpointed this partition is no longer alive, or the original RDD is
         |unpersisted. If this problem persists, you may consider using `rdd.checkpoint()`
         |instead, which is slower than local checkpointing but more fault-tolerant.
       """.stripMargin.replaceAll("\n", " "))
  }

  def endOfStreamError(): Throwable = {
    new java.util.NoSuchElementException("End of stream")
  }

  def cannotUseMapSideCombiningWithArrayKeyError(): Throwable = {
    new SparkException("Cannot use map-side combining with array keys.")
  }

  def hashPartitionerCannotPartitionArrayKeyError(): Throwable = {
    new SparkException("HashPartitioner cannot partition array keys.")
  }

  def reduceByKeyLocallyNotSupportArrayKeysError(): Throwable = {
    new SparkException("reduceByKeyLocally() does not support array keys")
  }

  def rddLacksSparkContextError(): Throwable = {
    new SparkException("This RDD lacks a SparkContext. It could happen in the following cases: " +
      "\n(1) RDD transformations and actions are NOT invoked by the driver, but inside of other " +
      "transformations; for example, rdd1.map(x => rdd2.values.count() * x) is invalid " +
      "because the values transformation and count action cannot be performed inside of the " +
      "rdd1.map transformation. For more information, see SPARK-5063.\n(2) When a Spark " +
      "Streaming job recovers from checkpoint, this exception will be hit if a reference to " +
      "an RDD not defined by the streaming job is used in DStream operations. For more " +
      "information, See SPARK-13758.")
  }

  def cannotChangeStorageLevelError(): Throwable = {
    new UnsupportedOperationException(
      "Cannot change storage level of an RDD after it was already assigned a level")
  }

  def canOnlyZipRDDsWithSamePartitionSizeError(): Throwable = {
    new SparkException("Can only zip RDDs with same number of elements in each partition")
  }

  def emptyCollectionError(): Throwable = {
    new UnsupportedOperationException("empty collection")
  }

  def countByValueApproxNotSupportArraysError(): Throwable = {
    new SparkException("countByValueApprox() does not support arrays")
  }

  def checkpointDirectoryHasNotBeenSetInSparkContextError(): Throwable = {
    new SparkException("Checkpoint directory has not been set in the SparkContext")
  }

  def invalidCheckpointFileError(path: Path): Throwable = {
    new SparkException(s"Invalid checkpoint file: $path")
  }

  def failToCreateCheckpointPathError(checkpointDirPath: Path): Throwable = {
    new SparkException(s"Failed to create checkpoint path $checkpointDirPath")
  }

  def checkpointRDDHasDifferentNumberOfPartitionsFromOriginalRDDError(
      originalRDDId: Int,
      originalRDDLength: Int,
      newRDDId: Int,
      newRDDLength: Int): Throwable = {
    new SparkException(
      s"""
         |Checkpoint RDD has a different number of partitions from original RDD. Original
         |RDD [ID: $originalRDDId, num of partitions: $originalRDDLength];
         |Checkpoint RDD [ID: $newRDDId, num of partitions: $newRDDLength].
       """.stripMargin.replaceAll("\n", " "))
  }

  def checkpointFailedToSaveError(task: Int, path: Path): Throwable = {
    new IOException("Checkpoint failed: failed to save output of task: " +
      s"$task and final output path does not exist: $path")
  }

  def mustSpecifyCheckpointDirError(): Throwable = {
    new SparkException("Checkpoint dir must be specified.")
  }

  def askStandaloneSchedulerToShutDownExecutorsError(e: Exception): Throwable = {
    new SparkException("Error asking standalone scheduler to shut down executors", e)
  }

  def stopStandaloneSchedulerDriverEndpointError(e: Exception): Throwable = {
    new SparkException("Error stopping standalone scheduler's driver endpoint", e)
  }

  def noExecutorIdleError(id: String): Throwable = {
    new NoSuchElementException(id)
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
    new SparkException("Can't run submitMapStage on RDD with 0 partitions")
  }

  def accessNonExistentAccumulatorError(id: Long): Throwable = {
    new SparkException(s"attempted to access non-existent accumulator $id")
  }

  def sendResubmittedTaskStatusForShuffleMapStagesOnlyError(): Throwable = {
    new SparkException("TaskSetManagers should only send Resubmitted task " +
      "statuses for tasks in ShuffleMapStages.")
  }

  def nonEmptyEventQueueAfterTimeoutError(timeoutMillis: Long): Throwable = {
    new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
  }

  def durationCalledOnUnfinishedTaskError(): Throwable = {
    new UnsupportedOperationException("duration() called on unfinished task")
  }

  def unrecognizedSchedulerModePropertyError(
      schedulerModeProperty: String,
      schedulingModeConf: String): Throwable = {
    new SparkException(s"Unrecognized $schedulerModeProperty: $schedulingModeConf")
  }

  def sparkError(errorMsg: String): Throwable = {
    new SparkException(errorMsg)
  }

  def clusterSchedulerError(message: String): Throwable = {
    new SparkException(s"Exiting due to error from cluster scheduler: $message")
  }

  def failToSerializeTaskError(e: Throwable): Throwable = {
    new TaskNotSerializableException(e)
  }

  def unrecognizedBlockIdError(name: String): Throwable = {
    new UnrecognizedBlockId(name)
  }

  def taskHasNotLockedBlockError(currentTaskAttemptId: Long, blockId: BlockId): Throwable = {
    new SparkException(s"Task $currentTaskAttemptId has not locked block $blockId for writing")
  }

  def blockDoesNotExistError(blockId: BlockId): Throwable = {
    new SparkException(s"Block $blockId does not exist")
  }

  def cannotSaveBlockOnDecommissionedExecutorError(blockId: BlockId): Throwable = {
    new BlockSavedOnDecommissionedBlockManagerException(blockId)
  }

  def waitingForReplicationToFinishError(e: Throwable): Throwable = {
    new SparkException("Error occurred while waiting for replication to finish", e)
  }

  def unableToRegisterWithExternalShuffleServerError(e: Throwable): Throwable = {
    new SparkException(s"Unable to register with external shuffle server due to : ${e.getMessage}",
      e)
  }

  def waitingForAsyncReregistrationError(e: Throwable): Throwable = {
    new SparkException("Error occurred while waiting for async. reregistration", e)
  }

  def unexpectedShuffleBlockWithUnsupportedResolverError(
      shuffleManager: ShuffleManager,
      blockId: BlockId): Throwable = {
    new SparkException(s"Unexpected shuffle block ${blockId} with unsupported shuffle " +
      s"resolver ${shuffleManager.shuffleBlockResolver}")
  }

  def failToStoreBlockOnBlockManagerError(
      blockManagerId: BlockManagerId,
      blockId: BlockId): Throwable = {
    new SparkException(s"Failure while trying to store block $blockId on $blockManagerId.")
  }

  def readLockedBlockNotFoundError(blockId: BlockId): Throwable = {
    new SparkException(s"Block $blockId was not found even though it's read-locked")
  }

  def failToGetBlockWithLockError(blockId: BlockId): Throwable = {
    new SparkException(s"get() failed for block $blockId even though we held a lock")
  }

  def blockNotFoundError(blockId: BlockId): Throwable = {
    new BlockNotFoundException(blockId.toString)
  }

  def interruptedError(): Throwable = {
    new InterruptedException()
  }

  def blockStatusQueryReturnedNullError(blockId: BlockId): Throwable = {
    new SparkException(s"BlockManager returned null for BlockStatus query: $blockId")
  }

  def unexpectedBlockManagerMasterEndpointResultError(): Throwable = {
    new SparkException("BlockManagerMasterEndpoint returned false, expected true.")
  }

  def failToCreateDirectoryError(path: String, maxAttempts: Int): Throwable = {
    new IOException(
      s"Failed to create directory ${path} with permission 770 after $maxAttempts attempts!")
  }

  def unsupportedOperationError(): Throwable = {
    new UnsupportedOperationException()
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
    new SparkException(s"Failed to get block $blockId, which is not a shuffle block", e)
  }
}
