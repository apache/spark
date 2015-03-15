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

private[python] class NestedDoubleArrayListWritable extends ArrayWritable(
  classOf[DoubleArrayWritable])

private[python] class NestedDoubleArrayWritable extends ArrayWritable(classOf[DoubleArrayWritable])


private[python] class DoubleArrayListToWritableConverter extends Converter[Any, Writable] {

  val converter = new DoubleArrayToWritableConverter

  override def convert(obj: Any) = obj match {
    case al: ArrayList[_] =>
      val list = asScalaBuffer(al).toList
      try {
        val aod = list.map(_.asInstanceOf[Double]).toArray
        converter.convert(aod)
      }
      catch {
        case cce: ClassCastException =>
          // ClassCastException is thrown only when cast fails on a non empty list.
          throw new SparkException(s"Data of type $list.head.getClass is not supported")
      }
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

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

private[python] class NestedDoubleArrayListToWritableConverter extends Converter[Any, Writable] {

  val nestedConverter = new DoubleArrayListToWritableConverter

  override def convert(obj: Any) = obj match {
    case al: ArrayList[_] =>
      val list = asScalaBuffer(al).toList
      try {
        val alod = list.map(e => e.asInstanceOf[ArrayList[Double]])
        val ndalw = new NestedDoubleArrayListWritable()
        ndalw.set(alod.map(nestedConverter.convert(_)).toArray)
        ndalw
      }
      catch {
        case cce: ClassCastException =>
        // ClassCastException is thrown only when cast fails on a non empty list.
        throw new SparkException(s"Data of type $list.head.getClass is not supported")
      }
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class NestedDoubleArrayToWritableConverter extends Converter[Any, Writable] {

  override def convert(obj: Any) = obj match {

    case obj if obj.getClass.isArray &&
      obj.getClass.getComponentType == classOf[Array[Double]] => {

      try {
        val objArray = obj.asInstanceOf[Array[_]]

        val arDaw = objArray.map(e => {
          val daw = new DoubleArrayWritable
          daw.set(e.asInstanceOf[Array[Double]].map(new DoubleWritable(_)))
          daw
        })

        val ndaw = new NestedDoubleArrayWritable
        ndaw.set(arDaw.map(_.asInstanceOf[Writable]))
        ndaw
      }
      catch {
        case e: ClassCastException => throw new SparkException(e.getMessage)
      }
    }
    case other =>
      throw new SparkException(s"Conversion failed")
  }
}

private[python] class WritableToNestedDoubleArrayListConverter extends
  Converter[Any, ArrayList[ArrayList[Double]]] {

  val nestedConverter = new WritableToDoubleArrayListConverter

  override def convert(obj: Any) = obj match {

    case ndaw: NestedDoubleArrayListWritable =>
      val converted = ndaw.get.map(nestedConverter.convert(_))
      val listNestedConverted = JavaConversions.mutableSeqAsJavaList[ArrayList[Double]](converted)
      new ArrayList[ArrayList[Double]](listNestedConverted);
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class WritableToNestedDoubleArrayConverter extends
  Converter[Any, Array[Array[Double]]] {

  override def convert(obj: Any) = obj match {

    case ndaw : NestedDoubleArrayWritable =>
      val daConverter = new WritableToDoubleArrayConverter
      ndaw.get.map(daConverter.convert(_))
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class WritableToDoubleArrayConverter extends Converter[Any, Array[Double]] {
  override def convert(obj: Any) = obj match {
    case daw : DoubleArrayWritable => daw.get.map(_.asInstanceOf[DoubleWritable].get)
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class WritableToDoubleArrayListConverter extends
  Converter[Any, ArrayList[Double]] {

  override def convert(obj: Any) = obj match {

    case daw : DoubleArrayWritable =>
      val arDouble = daw.get().map(_.asInstanceOf[DoubleWritable].get())
      val listDouble = JavaConversions.mutableSeqAsJavaList[Double](arDouble)
      new ArrayList[Double](listDouble)
    case other => throw new SparkException(s"Data of type $other is not support")
  }
}
