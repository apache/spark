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

package org.apache.spark.graphx.api.python

import java.io.{DataOutputStream, FileOutputStream}
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.spark.Accumulator
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.api.java.JavaVertexRDD
import org.apache.spark.rdd.RDD

private[graphx] class PythonVertexRDD(
    @transient parent: RDD[_],
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitioning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends PythonRDD (parent, command, envVars,
                     pythonIncludes, preservePartitioning,
                     pythonExec, broadcastVars, accumulator) {

  val asJavaVertexRDD = JavaVertexRDD.fromVertexRDD(VertexRDD(parent.asInstanceOf))

  def writeToFile[T](items: java.util.Iterator[T], filename: String) {
    import scala.collection.JavaConverters._
    writeToFile(items.asScala, filename)
  }

  def writeToFile[T](items: Iterator[T], filename: String) {
    val file = new DataOutputStream(new FileOutputStream(filename))
    writeIteratorToStream(items, file)
    file.close()
  }

  /** A data stream is written to a given file so that the collect() method
    * of class VertexRDD in Python can read it back in the client and
    * display the contents of the VertexRDD as a list
    */
  def writeIteratorToStream[T](items: Iterator[T], stream: DataOutputStream) = {
    if (items.hasNext) {
      val first = items.next()
      val newIter = Seq(first).iterator ++ items
      // Assuming the type of this RDD will always be Array[Byte]
      newIter.asInstanceOf[Iterator[Array[Byte]]].foreach { bytes =>
        stream.writeInt(bytes.length)
        stream.write(bytes)
      }
    }
  }
}

object PythonVertexRDD {
  val DEFAULT_SPARK_BUFFER_SIZE = 65536
}

class VertexProperty(val schemaString: String) {
  val schema : List[Any] = fromString(schemaString)

  /**
   * The vertex property schema is
   * @param schemaString
   * @return
   */
  def fromString(schemaString: String) : List[String] =
    schemaString.split(" ").toList
}
