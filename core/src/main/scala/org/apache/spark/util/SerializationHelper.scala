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

package org.apache.spark.util

import java.io.NotSerializableException
import java.nio.ByteBuffer

import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Task
import org.apache.spark.serializer.SerializerInstance

object SerializationState extends Enumeration {
  // Define vars to standardize debugging output
  type SerializationState = String
  var Failed = "Failed to serialize parent."
  var FailedDeps = "Failed to serialize dependencies."
  val Success = "Success"
}

/**
 * This class is designed to encapsulate some utilities to facilitate debugging serialization 
 * problems in the DAGScheduler and the TaskSetManager. See SPARK-3694.
 */
object SerializationHelper {
  type SerializedRdd = Either[String, ByteBuffer]

  /**
   * Helper function to check whether an RDD is serializable.
   *
   * If any dependency of an RDD is un-serializable, a NotSerializableException will be thrown
   * and the entire RDD will be deemed un-serializable if done with a single try-catch.
   *
   * Therefore, split the evaluation into two stages, in the first stage attempt to serialize 
   * the rdd. If it fails, attempt to serialize its dependencies in the failure handler and see 
   * if those also fail.
   *
   * This approach will show if any of the dependencies are un-serializable and will not 
   * incorrectly identify the parent RDD as being serializable.
   *
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param rdd - Rdd to attempt to serialize
   * @return SerializedRdd - If serialization is successful, return the serialized bytes, else 
   *                         return a String, which clarifies why things failed. 
   *
   *
   */
  def tryToSerialize(closureSerializer: SerializerInstance,
                     rdd: RDD[_]): SerializedRdd = {
    val result: SerializedRdd = try {
      Right(closureSerializer.serialize(rdd: AnyRef))
    } catch {
      case e: NotSerializableException => Left(handleFailure(closureSerializer, rdd))

      case NonFatal(e) => Left(handleFailure(closureSerializer, rdd))
    }

    result
  }

  /**
   * Helper function to separate an un-serializable parent rdd from un-serializable dependencies 
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param rdd - Rdd to attempt to serialize
   * @return String - Return a String (SerializationFailure), which clarifies why the serialization 
   *                 failed.
   */
  def handleFailure(closureSerializer: SerializerInstance,
                    rdd: RDD[_]): String = {
    if (rdd.dependencies.nonEmpty) {
      try {
        rdd.dependencies.foreach(dep => closureSerializer.serialize(dep: AnyRef))
        
        //By default return a parent failure since we know that the parent already failed
        SerializationState.Failed
      } catch {
        // If instead, however, the dependencies ALSO fail to serialize then the subsequent stage
        // of evaluation will help identify which of the dependencies has failed 
        case e: NotSerializableException => SerializationState.FailedDeps
        case NonFatal(e) => SerializationState.FailedDeps
      }
    }
    else {
      SerializationState.Failed
    }
  }

  /**
   * Provide a string representation of the task and its dependencies (in terms of added files
   * and jars that must be shipped with the task) for debugging purposes.
   * @param task - The task to serialize
   * @param addedFiles - The file dependencies
   * @param addedJars - The JAR dependencies
   * @return String - The task and dependencies as a string
   */
  def taskDebugString(task: Task[_],
                      addedFiles: HashMap[String, Long],
                      addedJars: HashMap[String, Long]): String = {
    val taskStr = "[" + task.toString + "] \n"
    val strPrefix = s"--  "
    val nl = s"\n"
    val fileTitle = s"File dependencies:$nl"
    val jarTitle = s"Jar dependencies:$nl"

    val fileStr = addedFiles.keys.map(file => s"$strPrefix $file").reduce(_ + nl + _) + nl
    val jarStr = addedJars.keys.map(jar => s"$strPrefix $jar").reduce(_ + nl + _) + nl

    s"$taskStr $nl $fileTitle $fileStr $jarTitle $jarStr"
  }
}

