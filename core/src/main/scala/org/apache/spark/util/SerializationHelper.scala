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

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.control.NonFatal

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.Task
import org.apache.spark.serializer.SerializerInstance

object SerializationState extends Enumeration {
  // Define vars to standardize debugging output
  type SerializationState = String
  val Failed = "Failed to serialize parent."
  val FailedDeps = "Failed to serialize dependencies."
  val Success = "Success"
}

/**
 * This class is designed to encapsulate some utilities to facilitate debugging serialization 
 * problems in the DAGScheduler and the TaskSetManager. See SPARK-3694.
 */
object SerializationHelper {
  type PathToRef = mutable.LinkedList[AnyRef]
  type BrokenRef = (AnyRef, PathToRef)
  type SerializedRef = Either[String, ByteBuffer]

  /**
   * Helper function to check whether a reference is serializable.
   *
   * If any dependency of an a reference is un-serializable, a NotSerializableException will be 
   * thrown and then we can execute a serialization trace to identify the problem reference.
   *
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param ref - The top-level reference that we are attempting to serialize 
   * @return SerializedRef - If serialization is successful, return success, else 
   *                         return a String, which clarifies why things failed.
   */
  def tryToSerialize(closureSerializer: SerializerInstance,
                     ref: AnyRef): SerializedRef = {
    val result: SerializedRef = try {
      Right(closureSerializer.serialize(ref))
    } catch {
      case e: NotSerializableException => Left(handleFailure(closureSerializer, ref))
      case NonFatal(e) => Left(handleFailure(closureSerializer, ref))
    }

    result
  }

  /**
   * Handle failures differently whether the passed reference is an RDD or something else since for 
   * RDDs we want to explicitly identify failures in dependencies
   * 
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param ref - The top-level reference that we are attempting to serialize
   * @return String - Return a String which clarifies why the serialization failed. Either a 
   *         SerializationState for an RDD or the reference trace for all other references
   */
  def handleFailure(closureSerializer: SerializerInstance,
                    ref: AnyRef) : String = {
    ref match {
      case rdd: RDD[_] =>
        handleFailedRdd(closureSerializer, rdd)
      case _ =>
        getSerializationTrace(closureSerializer,ref)
    }
  }

  /**
   * Helper function to separate an un-serializable parent rdd from un-serializable dependencies
   * 
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param rdd - Rdd to attempt to serialize
   * @return String - Return a String (SerializationFailure), which clarifies why the serialization 
   *                 failed.
   */
  def handleFailedRdd(closureSerializer: SerializerInstance,
                    rdd: RDD[_]): String = {
    if (rdd.dependencies.nonEmpty) {
      try {
        rdd.dependencies.foreach(dep => closureSerializer.serialize(dep: AnyRef))

        // By default return a parent failure since we know that the parent already failed
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
   * When an RDD is identified as un-serializable, use the generic ObjectWalker class to debug 
   * the references of that RDD and generate a set of paths to broken references
   *
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param ref - The reference known to be un-serializable
   * @return a Set of (AnyRef, LinkedList) - a tuple of the un-serialiazble reference and the 
   *         path to that reference
   */
  def getPathsToBrokenRefs(closureSerializer: SerializerInstance,
                           ref: AnyRef) : mutable.Set[BrokenRef] = {
    val refGraph : mutable.LinkedList[AnyRef] = ObjectWalker.buildRefGraph(ref)
    val brokenRefs = mutable.Set[BrokenRef]()

    refGraph.foreach {
      case ref : AnyRef =>
        try {
          closureSerializer.serialize(ref)
        } catch {
          case e: NotSerializableException => brokenRefs.add(ref, ObjectWalker.buildRefGraph(ref))
          case NonFatal(e) => brokenRefs.add(ref, ObjectWalker.buildRefGraph(ref))
        }
    }

    brokenRefs
  }
  
  /**
   * Returns nicely formatted text representing the trace of the failed serialization
   * 
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param ref - The top-level reference that we are attempting to serialize 
   * @return
   */
  def getSerializationTrace(closureSerializer: SerializerInstance,
                              ref : AnyRef) : String = {
    var trace = "Un-serializable reference trace for " + ref.toString + ":\n"
    trace += brokenRefsToString(getPathsToBrokenRefs(closureSerializer, ref))
    trace
  }

  def refString(ref : AnyRef) : String = {
    val refCode = System.identityHashCode(ref)
    "Ref (" + ref.toString + ", Hash: " + refCode + ")"
  }

  /**
   * Given a set of reference and the paths to those references (as a dependency tree), return 
   * a cleanly formatted string showing these paths.
   * 
   * @param brokenRefPath - a tuple of the un-serialiazble reference and the path to that reference
   */
  def brokenRefsToString(brokenRefPath : mutable.Set[BrokenRef]) : String = {
    var trace = "**********************\n"  
    
    brokenRefPath.foreach(s => trace += brokenRefToString(s) + "**********************\n")
    trace
  }
  
  /**
   * Given a reference and a path to that reference (as a dependency tree), return a cleanly 
   * formatted string showing this path. 
   * @param brokenRefPath - a tuple of the un-serialiazble reference and the path to that reference
   */
  def brokenRefToString(brokenRefPath : (AnyRef, mutable.LinkedList[AnyRef])) : String = {
    val ref = brokenRefPath._1
    val path = brokenRefPath._2
    
    var trace = ref + ":\n"
    path.foreach(s => {
      trace += "--- " + refString(s) +  "\n"  
    })
    
    trace
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

