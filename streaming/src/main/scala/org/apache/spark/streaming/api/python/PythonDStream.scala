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

package org.apache.spark.streaming.api.python

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.Proxy
import java.util.{ArrayList => JArrayList, List => JList}
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.spark.api.java._
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._


/**
 * Interface for Python callback function with three arguments
 */
private[python] trait PythonTransformFunction {
  def call(time: Long, rdds: JList[_]): JavaRDD[Array[Byte]]
}

/**
 * Wrapper for PythonTransformFunction
 * TODO: support checkpoint
 */
private[python] class TransformFunction(@transient var pfunc: PythonTransformFunction)
  extends function.Function2[JList[JavaRDD[_]], Time, JavaRDD[Array[Byte]]] with Serializable {

  def apply(rdd: Option[RDD[_]], time: Time): Option[RDD[Array[Byte]]] = {
    Option(pfunc.call(time.milliseconds, List(rdd.map(JavaRDD.fromRDD(_)).orNull).asJava)).map(_.rdd)
  }

  def apply(rdd: Option[RDD[_]], rdd2: Option[RDD[_]], time: Time): Option[RDD[Array[Byte]]] = {
    val rdds = List(rdd.map(JavaRDD.fromRDD(_)).orNull, rdd2.map(JavaRDD.fromRDD(_)).orNull).asJava
    Option(pfunc.call(time.milliseconds, rdds)).map(_.rdd)
  }

  // for function.Function2
  def call(rdds: JList[JavaRDD[_]], time: Time): JavaRDD[Array[Byte]] = {
    pfunc.call(time.milliseconds, rdds)
  }

  private def writeObject(out: ObjectOutputStream): Unit = {
    assert(PythonDStream.serializer != null, "Serializer has not been registered!")
    val bytes = PythonDStream.serializer.serialize(pfunc)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    assert(PythonDStream.serializer != null, "Serializer has not been registered!")
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    pfunc = PythonDStream.serializer.deserialize(bytes)
  }
}

/**
 * Interface for Python Serializer to serialize PythonTransformFunction
 */
private[python] trait PythonTransformFunctionSerializer {
  def dumps(id: String): Array[Byte]  //
  def loads(bytes: Array[Byte]): PythonTransformFunction
}

/**
 * Wrapper for PythonTransformFunctionSerializer
 */
private[python] class TransformFunctionSerializer(pser: PythonTransformFunctionSerializer) {
  def serialize(func: PythonTransformFunction): Array[Byte] = {
    // get the id of PythonTransformFunction in py4j
    val h = Proxy.getInvocationHandler(func.asInstanceOf[Proxy])
    val f = h.getClass().getDeclaredField("id")
    f.setAccessible(true)
    val id = f.get(h).asInstanceOf[String]
    pser.dumps(id)
  }

  def deserialize(bytes: Array[Byte]): PythonTransformFunction = {
    pser.loads(bytes)
  }
}

/**
 * Helper functions
 */
private[python] object PythonDStream {

  // A serializer in Python, used to serialize PythonTransformFunction
  var serializer: TransformFunctionSerializer = _

  // Register a serializer from Python, should be called during initialization
  def registerSerializer(ser: PythonTransformFunctionSerializer) = {
    serializer = new TransformFunctionSerializer(ser)
  }

  // helper function for DStream.foreachRDD(),
  // cannot be `foreachRDD`, it will confusing py4j
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], pfunc: PythonTransformFunction) {
    val func = new TransformFunction((pfunc))
    jdstream.dstream.foreachRDD((rdd, time) => func(Some(rdd), time))
  }

  // convert list of RDD into queue of RDDs, for ssc.queueStream()
  def toRDDQueue(rdds: JArrayList[JavaRDD[Array[Byte]]]): java.util.Queue[JavaRDD[Array[Byte]]] = {
    val queue = new java.util.LinkedList[JavaRDD[Array[Byte]]]
    rdds.forall(queue.add(_))
    queue
  }
}

/**
 * Base class for PythonDStream with some common methods
 */
private[python]
abstract class PythonDStream(parent: DStream[_], @transient pfunc: PythonTransformFunction)
  extends DStream[Array[Byte]] (parent.ssc) {

  val func = new TransformFunction(pfunc)

  override def dependencies = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  val asJavaDStream  = JavaDStream.fromDStream(this)
}

/**
 * Transformed DStream in Python.
 *
 * If `reuse` is true and the result of the `func` is an PythonRDD, then it will cache it
 * as an template for future use, this can reduce the Python callbacks.
 */
private[python]
class PythonTransformedDStream (parent: DStream[_], @transient pfunc: PythonTransformFunction,
                                var reuse: Boolean = false)
  extends PythonDStream(parent, pfunc) {

  // rdd returned by func
  var lastResult: PythonRDD = _

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isEmpty) {
      return None
    }
    if (reuse && lastResult != null) {
      // use the previous result as the template to generate new RDD
      Some(lastResult.copyTo(rdd.get))
    } else {
      val r = func(rdd, validTime)
      if (reuse && r.isDefined && lastResult == null) {
        // try to use the result as a template
        r.get match {
          case pyrdd: PythonRDD =>
            if (pyrdd.parent(0) == rdd) {
              // only one PythonRDD
              lastResult = pyrdd
            } else {
              // maybe have multiple stages, don't check it anymore
              reuse = false
            }
        }
      }
      r
    }
  }
}

/**
 * Transformed from two DStreams in Python.
 */
private[python]
class PythonTransformed2DStream(parent: DStream[_], parent2: DStream[_],
                                @transient pfunc: PythonTransformFunction)
  extends DStream[Array[Byte]] (parent.ssc) {

  val func = new TransformFunction(pfunc)

  override def slideDuration: Duration = parent.slideDuration

  override def dependencies = List(parent, parent2)

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    func(parent.getOrCompute(validTime), parent2.getOrCompute(validTime), validTime)
  }

  val asJavaDStream = JavaDStream.fromDStream(this)
}

/**
 * similar to StateDStream
 */
private[python]
class PythonStateDStream(parent: DStream[Array[Byte]], @transient reduceFunc: PythonTransformFunction)
  extends PythonDStream(parent, reduceFunc) {

  super.persist(StorageLevel.MEMORY_ONLY)
  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      func(lastState, rdd, validTime)
    } else {
      lastState
    }
  }
}

/**
 * similar to ReducedWindowedDStream
 */
private[python]
class PythonReducedWindowedDStream(parent: DStream[Array[Byte]],
                                   @transient preduceFunc: PythonTransformFunction,
                                   @transient pinvReduceFunc: PythonTransformFunction,
                                   _windowDuration: Duration,
                                   _slideDuration: Duration
                                   ) extends PythonDStream(parent, preduceFunc) {

  super.persist(StorageLevel.MEMORY_ONLY)
  override val mustCheckpoint = true

  val invReduceFunc = new TransformFunction(pinvReduceFunc)

  def windowDuration: Duration = _windowDuration
  override def slideDuration: Duration = _slideDuration
  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val currentTime = validTime
    val current = new Interval(currentTime - windowDuration,
      currentTime)
    val previous = current - slideDuration

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //

    val previousRDD = getOrCompute(previous.endTime)

    if (pinvReduceFunc != null && previousRDD.isDefined
        // for small window, reduce once will be better than twice
        && windowDuration >= slideDuration * 5) {

      // subtract the values from old RDDs
      val oldRDDs = parent.slice(previous.beginTime + parent.slideDuration, current.beginTime)
      val subtracted = if (oldRDDs.size > 0) {
        invReduceFunc(previousRDD, Some(ssc.sc.union(oldRDDs)), validTime)
      } else {
        previousRDD
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs = parent.slice(previous.endTime + parent.slideDuration, current.endTime)
      if (newRDDs.size > 0) {
        func(subtracted, Some(ssc.sc.union(newRDDs)), validTime)
      } else {
        subtracted
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs = parent.slice(current.beginTime + parent.slideDuration, current.endTime)
      if (currentRDDs.size > 0) {
        func(None, Some(ssc.sc.union(currentRDDs)), validTime)
      } else {
        None
      }
    }
  }
}
