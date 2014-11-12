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

import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import org.apache.spark.graphx.api.java.JavaVertexRDD
import org.apache.spark.rdd.RDD

//class PythonVertexRDD (
//    parent: JavaRDD[Array[Byte]],
//    command: Array[Byte],
//    envVars: JMap[String, String],
//    pythonIncludes: JList[String],
//    preservePartitoning: Boolean,
//    pythonExec: String,
//    broadcastVars: JList[Broadcast[Array[Byte]]],
//    accumulator: Accumulator[JList[Array[Byte]]],
//    targetStorageLevel: String = "MEMORY_ONLY")
//  extends RDD[Array[Byte]](parent) {

class PythonVertexRDD(parent: RDD[_], schema: String) extends {

  /**
   * Implemented by subclasses to return the set of partitions in this RDD. This method will only
   * be called once, so it is safe to implement a time-consuming computation in it.
   */
//  override def getPartitions: Array[Partition] = ???

//  def this(parent: JavaRDD[Array[Byte]], command: String, preservePartitioning: Boolean) {
//  def this(parent: JavaRDD[Array[Byte]], command: String, preservePartitioning: Boolean) {
//    this(parent, null, null, preservePartitioning, "MEMORY_ONLY")
//    System.out.println("PythonVertexRDD constructor")
//  }

  val asJavaVertexRDD = JavaVertexRDD.fromVertexRDD(parent.asInstanceOf)

  def toVertexRDD[VD](pyRDD: RDD[_], schema: String): JavaVertexRDD[Array[Byte]] = {
//    new VertexRDD[VD](PythonRDD.pythonToJava(pyRDD, true), StorageLevel.MEMORY_ONLY)
    System.out.println("In PythonVertexRDD.toVertexRDD()")
    val propertySchema = new VertexProperty(schema)
    val vertices = new JavaVertexRDD[VertexProperty](pyRDD.asInstanceOf)
    null
  }
}

object PythonVertexRDD {
  val DEFAULT_SPARK_BUFFER_SIZE = 65536

  def toVertexRDD(parent: RDD[_], schema: String) : JavaVertexRDD[Array[Byte]] = {
    val pyRDD = new PythonVertexRDD(parent, schema)
    pyRDD.toVertexRDD(parent, schema)
  }
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
