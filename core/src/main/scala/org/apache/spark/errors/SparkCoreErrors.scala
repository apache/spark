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

import org.apache.spark.{SparkException, SparkUserAppException, TaskContext, TaskKilledException}
import org.apache.spark.deploy.rest.{SubmitRestConnectionException, SubmitRestMissingFieldException, SubmitRestProtocolException}
import org.apache.spark.internal.config.History
import org.apache.spark.scheduler.HaltReplayException

/**
 * Object for grouping error messages from (most) exceptions thrown during query execution.
 */
object SparkCoreErrors {
  def failedGetIntoAcceptableClusterStateError(e: TimeoutException): Throwable = {
    new RuntimeException("Failed to get into acceptable cluster state after 2 min.", e)
  }

  def serverSocketFailedBindJavaSideError(message: String): Throwable = {
    new SparkException(message)
  }

  def taskKilledUnknownReasonError(context: TaskContext): Throwable = {
    new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))
  }

  def pythonWorkerExitedError(error: String, eof: EOFException): Throwable = {
    new SparkException(s"Python worker exited unexpectedly (crashed): $error", eof)
  }

  def pythonWorkerExitedError(eof: EOFException): Throwable = {
    new SparkException("Python worker exited unexpectedly (crashed)", eof)
  }

  def sparkRBackendInitializeError(message: String): Throwable = {
    new SparkException(message)
  }

  def sparkUserAppError(returnCode: Int): Throwable = {
    SparkUserAppException(returnCode)
  }

  def keytabFileNotExistError(keytabFilename: String): Throwable = {
    new SparkException(s"Keytab file: ${keytabFilename} does not exist")
  }

  def failedCreateParentsError(path: Path): Throwable = {
    new IOException(s"Failed to create parents of $path")
  }

  def failedLoadIvySettingError(settingsFile: String, e: Throwable): Throwable = {
    new SparkException(s"Failed when loading Ivy settings from $settingsFile", e)
  }

  def lackSparkConfigError(pair: String): Throwable = {
    new SparkException(s"Spark config without '=': $pair")
  }

  def multipleExternalSparkSubmitOperationsRegisteredError(x: Int, master: String): Throwable = {
    new SparkException(s"Multiple($x) external SparkSubmitOperations " +
      s"clients registered for master url ${master}.")
  }

  def securityError(): Throwable = {
    new SecurityException()
  }

  def preparingResourceFileError(compShortName: String, e: Throwable): Throwable = {
    new SparkException(s"Exception threw while preparing resource file for $compShortName", e)
  }

  def noApplicationWithApplicationIdError(appId: String, attemptId: Option[String]): Throwable = {
    new NoSuchElementException(s"no application with application Id '$appId'" +
      attemptId.map { id => s" attemptId '$id'" }.getOrElse(" and no attempt Id"))
  }

  def filterOnlyWorksForHTTPHTTPSError(): Throwable = {
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

  def noSuchElementError(): Throwable = {
    new NoSuchElementException()
  }

  def noSuchElementError(s: String): Throwable = {
    new NoSuchElementException(s)
  }

  def notFoundLogForAppIdError(appId: String): Throwable = {
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

  def unableConnectServerError(e: Throwable): Throwable = {
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

  def failedValidateMessageError(messageType: String, e: Exception): Throwable = {
    new SubmitRestProtocolException(s"Validation of message $messageType failed!", e)
  }

  def missActionFieldError(messageType: String): Throwable = {
    new SubmitRestMissingFieldException(s"The action field is missing in $messageType")
  }

  def missFieldInMessageError(messageType: String, name: String): Throwable = {
    new SubmitRestMissingFieldException(s"'$name' is missing in message $messageType.")
  }

  def notFoundActionFieldInJSONError(json: String): Throwable = {
    new SubmitRestMissingFieldException(s"Action field not found in JSON:\n$json")
  }

  def unexpectedValueForPropertyError(key: String, valueType: String, value: String): Throwable = {
    new SubmitRestProtocolException(
      s"Property '$key' expected $valueType value: actual was '$value'.")
  }

  def cannotGetMasterKerberosPrincipalRenewerError(): Throwable = {
    new SparkException("Can't get Master Kerberos principal for use as renewer.")
  }

  def failedCreateDirectoryError(driverDir: File): Throwable = {
    new IOException("Failed to create directory " + driverDir)
  }

  def cannotFindJarInDriverDirectoryError(jarFileName: String, driverDir: File): Throwable = {
    new IOException(
      s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
  }

  def failedListFilesInAppDirsError(appDirs: Array[File]): Throwable = {
    new IOException("ERROR: Failed to list files in " + appDirs)
  }

  def cannotCreateSubfolderInLocalRootDirsError(localRootDirs: String): Throwable = {
    new IOException(s"No subfolder can be created in ${localRootDirs}.")
  }

  def requestMustSpecifyApplicationOrDriverError(): Throwable = {
    new Exception("Request must specify either application or driver identifiers")
  }
}
