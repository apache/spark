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

import java.io.EOFException

import org.apache.spark.{SparkException, TaskKilledException}
import org.apache.spark.api.r.JVMObjectId

private[spark] object ExecutionErrors {
  def juRemoveError(): Throwable = {
    new UnsupportedOperationException("remove")
  }

  def unexpectedPy4JServerError(other: Object): Throwable = {
    new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }

  def cannotBeUsedDataOfTypeError(className: String): Throwable = {
    new SparkException(s"Data of type $className cannot be used")
  }

  def unexpectedValuePairwiseRDDError(x: Seq[Array[Byte]]): Throwable = {
    new SparkException("PairwiseRDD: unexpected value: " + x)
  }

  def unexpectedElementTypeError(other: Object): Throwable = {
    new SparkException("Unexpected element type " + other.getClass)
  }

  def eofBeforePythonServerAcknowledgedError(): Throwable = {
    new SparkException("EOF reached before Python server acknowledged")
  }

  def taskInterruptionError(reason: String): Throwable = {
    new TaskKilledException(reason)
  }

  def crashedUnexpectedlyPythonWorkerError(eof: EOFException): Throwable = {
    new SparkException("Python worker exited unexpectedly (crashed)", eof)
  }

  def serverSocketFailedError(message: String): Throwable = {
    new SparkException(message)
  }

  def invalidPortNumberError(exceptionMessage: String): Throwable = {
    new SparkException(exceptionMessage)
  }

  def failedToConnectBackPythonWorkerError(e: Exception): Throwable = {
    new SparkException("Python worker failed to connect back.", e)
  }

  def eofExceptionWhileReadPortNumberFromAliveDaemonError(daemonModule: String): Throwable = {
    new SparkException("EOFException occurred while reading the port number " +
      s"from $daemonModule's stdout")
  }

  def eofExceptionWhileReadPortNumberError(
      daemonModule: String,
      daemonExitValue: Int): Throwable = {
    new SparkException(
      s"EOFException occurred while reading the port number from $daemonModule's" +
        s" stdout and terminated with code: $daemonExitValue.")
  }

  def cannotBeUsedRDDElementError(otherName: String): Throwable = {
    new SparkException(s"RDD element of type $otherName cannot be used")
  }

  def unsupportedDataTypeError(other: Any): Throwable = {
    new SparkException(s"Data of type $other is not supported")
  }

  def workerProducedError(msg: String, e: Exception): Throwable = {
    new SparkException(msg, e)
  }

  def jvmObjectIdNotExistError(id: JVMObjectId): Throwable = {
    new NoSuchElementException(s"$id does not exist.")
  }

  def noMatchedMethodFoundError(cls: Object, methodName: String): Throwable = {
    new Exception(s"No matched method found for $cls.$methodName")
  }

  def noMatchedConstructorFoundError(cls: Object): Throwable = {
    new Exception(s"No matched constructor found for $cls")
  }

  def cannotLocateSparkRPackage(): Throwable = {
    new SparkException("SPARK_HOME not set. Can't locate SparkR package.")
  }

  def keyInMapCannotBeNullError(): Throwable = {
    new IllegalArgumentException("Key in map can't be null.")
  }

  def invalidMapKeyTypeError(key: String): Throwable = {
    new IllegalArgumentException(s"Invalid map key type: $key")
  }

  def invalidArrayTypeError(arrType: Char): Throwable = {
    new IllegalArgumentException (s"Invalid array type $arrType")
  }
}
