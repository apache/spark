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
import scala.collection.mutable.{ArrayBuffer, HashMap}
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
   * Returns an array of SerializedRdd objects (which are either the successfully serialized RDD and 
   * its dependencies or a failure description), as nicely formatted text. 
   *
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param rdd - The top-level rdd that we are attempting to serialize 
   * @param results - The result of attempting to serialize that rdd
  
   * @return
   */
  def getSerializationTrace(closureSerializer: SerializerInstance,
                              rdd : RDD[_], 
                              results : Array[SerializedRdd]) = {
    var trace = "RDD serialization trace:\n"
    val it = results.iterator
    
    while (it.hasNext) {
      trace += rdd.name + ": " + it.next().fold(l => {
        // When we know that this RDD was explicitly the one that failed, print out the 
        // path to the broken refs
        if (l.equals(SerializationState.Failed)) {
          l + "\n" + brokenRefsToString(getPathsToBrokenRefs(closureSerializer, rdd)) 
        } else {
          l
        }
         
      }, r=> SerializationState.Success) + "\n"
    }
    trace
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
        
        // By default return a parent failure since we now know that the dependency is serializable
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
   * @param closureSerializer - An instance of a serializer (single-threaded) that will be used
   * @param rdd - The RDD that is known to be un-serializable
   * @return a Set of (AnyRef, ArrayBuffer) - a tuple of the unserialiazble reference and the 
   *         path to that reference
   */
  def getPathsToBrokenRefs(closureSerializer: SerializerInstance,
                           rdd: RDD[_]) : mutable.Set[(AnyRef, ArrayBuffer[AnyRef])]= {
    val refGraph : mutable.Set[ObjectWalker.EdgeRef] = ObjectWalker.buildRefGraph(rdd)
    val brokenRefs = mutable.Set[(AnyRef, ArrayBuffer[AnyRef])]()
    
    refGraph.foreach {
      case ref : ObjectWalker.EdgeRef => 
        try {
          closureSerializer.serialize(ref.cur)
        } catch {
          // If instead, however, the dependencies ALSO fail to serialize then the subsequent stage
          // of evaluation will help identify which of the dependencies has failed 
          case e: NotSerializableException => brokenRefs.add(ref, ObjectWalker.pathToBrokenRef(ref))
          case NonFatal(e) => brokenRefs.add(ref, ObjectWalker.pathToBrokenRef(ref))
        }
    }
    brokenRefs
  }

  def refString(ref : AnyRef) : String= {
    val refCode = System.identityHashCode(ref)
    "Ref (" + ref.getClass + ", " + refCode + ")"
  }

  /**
   * Given a set of reference and the paths to those references (as a dependency tree), return 
   * a cleanly formatted string showing these path. 
   * @param brokenRefPath - a tuple of the un-serialiazble reference and the path to that reference
   */
  def brokenRefsToString(brokenRefPath : mutable.Set[(AnyRef, ArrayBuffer[AnyRef])]) : String = {
    var trace = "**********************"  
    brokenRefPath.foreach(trace += brokenRefToString(_) + "**********************\n")
    trace
  }
  
  /**
   * Given a reference and a path to that reference (as a dependency tree), return a cleanly 
   * formatted string showing this path. 
   * @param brokenRefPath - a tuple of the un-serialiazble reference and the path to that reference
   */
  def brokenRefToString(brokenRefPath : (AnyRef, ArrayBuffer[AnyRef])) : String = {
    val ref = brokenRefPath._1
    val path = brokenRefPath._2
    
    var trace = "Un-serializable reference trace for " + ref + " :\n"
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

