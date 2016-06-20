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

package org.apache.spark.ml.api.python

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.Proxy

import org.apache.hadoop.fs.Path
import org.json4s._

import org.apache.spark.SparkException
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

trait PythonTransformerWrapper {
  def getUid: String

  def transform(dataset: Dataset[_]): DataFrame

  def transformSchema(schema: StructType): StructType

  def getTransformer: Array[Byte]

  def getClassName: String

  def save(path: String): Unit

  def copy(extra: ParamMap): PythonTransformerWrapper

  def getLastFailure: String
}

object PythonTransformerWrapper {
  private var reader: PythonTransformerWrapperReader = _

  def registerReader(r: PythonTransformerWrapperReader): Unit = {
    reader = r
  }

  def load(path: String, clazz: String): PythonTransformerWrapper = {
    require(reader != null, "Python reader has not been registered.")
    callLoadFromPython(path, clazz)
  }

  def callLoadFromPython(path: String, clazz: String): PythonTransformerWrapper = {
    val result = reader.load(path, clazz)
    val failure = reader.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    result
  }
}

trait PythonTransformerWrapperReader {
  def getLastFailure: String
  def load(path: String, clazz: String): PythonTransformerWrapper
}

trait PythonTransformerWrapperSerializer {
  def dumps(id: String): Array[Byte]
  def loads(bytes: Array[Byte]): PythonTransformerWrapper

  /**
   * Get the failure, if any, in the last call to `dumps` or `loads`.
   *
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String
}

/**
 * Helpers for PythonTransformFunctionSerializer
 *
 * PythonTransformFunctionSerializer is logically a singleton that's happens to be
 * implemented as a Python object.
 */
private[python] object PythonTransformerWrapperSerializer {

  /**
   * A serializer in Python, used to serialize PythonTransformFunction
    */
  private var serializer: PythonTransformerWrapperSerializer = _

  /*
   * Register a serializer from Python, should be called during initialization
   */
  def register(ser: PythonTransformerWrapperSerializer): Unit = synchronized {
    serializer = ser
  }

  def serialize(wrapper: PythonTransformerWrapper): Array[Byte] = synchronized {
    require(serializer != null, "Serializer has not been registered!")
    // get the id of PythonTransformFunction in py4j
    val h = Proxy.getInvocationHandler(wrapper.asInstanceOf[Proxy])
    val f = h.getClass().getDeclaredField("id")
    f.setAccessible(true)
    val id = f.get(h).asInstanceOf[String]
    val results = serializer.dumps(id)
    val failure = serializer.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    results
  }

  def deserialize(bytes: Array[Byte]): PythonTransformerWrapper = synchronized {
    require(serializer != null, "Serializer has not been registered!")
    val wrapper = serializer.loads(bytes)
    val failure = serializer.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    wrapper
  }
}

class PythonTransformer(
    @transient var pfunc: PythonTransformerWrapper,
    override val uid: String) extends Transformer with MLWritable {

  override def transformSchema(schema: StructType): StructType = {
    pfunc.transformSchema(schema)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    callTransformFromPython(dataset)
  }

  def getPythonTransformer: Array[Byte] = {
    callGetTransformerFromPython
  }

  def getPythonClassName: String = {
    pfunc.getClassName
  }

  override def copy(extra: ParamMap): PythonTransformer = {
    this.pfunc = this.pfunc.copy(extra)
    this
  }

  override def write: MLWriter = new PythonTransformer.PythonTransformerWriter(this)

  def callGetTransformerFromPython: Array[Byte] = {
    val result = pfunc.getTransformer
    val failure = pfunc.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    result
  }

  def callTransformFromPython(dataset: Dataset[_]): DataFrame = {
    val result = pfunc.transform(dataset)
    val failure = pfunc.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    result
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val bytes = PythonTransformerWrapperSerializer.serialize(pfunc)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    pfunc = PythonTransformerWrapperSerializer.deserialize(bytes)
  }
}

object PythonTransformer extends MLReadable[PythonTransformer] {
  override def read: MLReader[PythonTransformer] = new PythonTransformerReader

  override def load(path: String): PythonTransformer = super.load(path)

  class PythonTransformerWriter(instance: PythonTransformer) extends MLWriter {
    override def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "pyClass" -> instance.getPythonClassName
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val pyDir = new Path(path, "pyTransformer").toString
      instance.pfunc.save(pyDir)
      val failure = instance.pfunc.getLastFailure
      if (failure != null) {
        throw new SparkException("An exception was raised by Python:\n" + failure)
      }
    }
  }

  class PythonTransformerReader extends MLReader[PythonTransformer] {
    private val className = classOf[PythonTransformer].getName
    override def load(path: String): PythonTransformer = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val pyClass = (metadata.metadata \ "pyClass").extract[String]
      val pyDir = new Path(path, "pyTransformer").toString
      val pfunc = PythonTransformerWrapper.load(pyDir, pyClass)
      new PythonTransformer(pfunc, pfunc.getUid)
    }
  }
}

object PythonPipelineStage {
  // Helper function due to Py4J error of reader/serializer does not exist in the JVM.
  def registerReader(r: PythonTransformerWrapperReader): Unit = {
    PythonTransformerWrapper.registerReader(r)
  }

  def registerSerializer(ser: PythonTransformerWrapperSerializer): Unit = {
    PythonTransformerWrapperSerializer.register(ser)
  }
}
