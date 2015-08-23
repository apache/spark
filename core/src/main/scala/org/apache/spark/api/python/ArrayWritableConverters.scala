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

import org.apache.hadoop.io._
import org.apache.spark.SparkException

private[python] class DoubleArrayWritable extends ArrayWritable(classOf[DoubleWritable])

private[python] class BytesArrayWritable extends ArrayWritable(classOf[BytesWritable])

private[python] class ByteArrayToWritableConverter extends Converter[Any, Writable] {

    override def convert(obj: Any) = obj match {
      case aob: Array[Byte] => new BytesWritable(aob)
      case other =>
        val simpleName = other.getClass.getSimpleName
        throw new SparkException(s"Data of type $simpleName is not supported")
    }
}

private[python] class WritableToByteArrayConverter extends Converter[Any, Array[Byte]] {

  override def convert(obj: Any) = obj match {
    case bw : BytesWritable => bw.getBytes
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}

private[python] class DoubleArrayToWritableConverter extends Converter[Any, Writable] {

  override def convert(obj: Any) = obj match {
      case arr: Array[Double] =>
        val daw = new DoubleArrayWritable
        daw.set(arr.asInstanceOf[Array[Double]].map(new DoubleWritable(_)))
        daw
      case other => throw new SparkException(s"Data of type $other is not supported")
    }
}

private[python] class WritableToDoubleArrayConverter extends Converter[Any, Array[Double]] {

  override def convert(obj: Any) = obj match {
    case daw : DoubleArrayWritable => daw.get.map(_.asInstanceOf[DoubleWritable].get)
    case other => throw new SparkException(s"Data of type $other is not supported")
  }
}
