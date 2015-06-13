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

package org.apache.spark.api.python

import java.util._

import org.apache.hadoop.io.{Writable, DoubleWritable, ArrayWritable}
import org.apache.spark.SparkException

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

private[python] class DoubleArrayListWritable extends ArrayWritable(
  classOf[DoubleArrayListWritable])

private[python] class DoubleArrayWritable extends ArrayWritable(classOf[DoubleWritable])

private[python] class NestedDoubleArrayWritable extends ArrayWritable(classOf[DoubleArrayWritable])

private[python] class DoubleArrayToWritableConverter extends Converter[Any, Writable] {

  override def convert(obj: Any) = {

    obj match {
      case arr if arr.getClass.isArray && arr.getClass.getComponentType == classOf[Double] =>
        val daw = new DoubleArrayWritable
        daw.set(arr.asInstanceOf[Array[Double]].map(new DoubleWritable(_)))
        daw
      case other => throw new SparkException(s"Data of type $other is not supported")
    }
  }
}

private[python] class WritableToDoubleArrayConverter extends Converter[Any, Array[Double]] {
  override def convert(obj: Any) = obj match {
    case daw : DoubleArrayWritable => daw.get.map(_.asInstanceOf[DoubleWritable].get)
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class DoubleArrayListOfDoubleArrayToWritableConverter extends
Converter[Any, Writable] {

  val nestedConverter = new DoubleArrayToWritableConverter

  override def convert(obj: Any) = obj match {

    case al: ArrayList[_] =>
      val list = asScalaBuffer(al).toList
      try {
        val aod = list.map(e => e.asInstanceOf[Array[Double]])
        val ndalw = new NestedDoubleArrayWritable()
        ndalw.set(aod.map(nestedConverter.convert(_)).toArray)
        ndalw
      }
      catch {
        case cce: ClassCastException =>
          // ClassCastException is thrown only when cast fails on a non empty list.
          val listElemClassName = list.head.getClass.getCanonicalName
          throw new SparkException(s"Data of type $listElemClassName is not supported")
      }
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class WritableToDoubleArrayListOfDoubleArrayConverter extends
Converter[Any, ArrayList[Array[Double]]] {

  val nestedConverter = new WritableToDoubleArrayConverter

  override def convert(obj: Any) = obj match {
    case ndalw: NestedDoubleArrayWritable =>
      val converted = ndalw.get.map(nestedConverter.convert(_))
      val listNestedConverted = JavaConversions.mutableSeqAsJavaList[Array[Double]](converted)
      new ArrayList[Array[Double]](listNestedConverted)
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}