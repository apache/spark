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
import org.apache.spark.ml.{Estimator, Model, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Wrapper of transformers written in pure Python. Its implementation is in PySpark.
 * See pyspark.ml.util.TransformerWrapper
 */
private[python] trait PythonStageWrapper {

  def getUid: String

  def fit(dataset: Dataset[_]): PythonStageWrapper

  def transform(dataset: Dataset[_]): DataFrame

  def transformSchema(schema: StructType): StructType

  def getStage: Array[Byte]

  def getClassName: String

  def save(path: String): Unit

  def copy(extra: ParamMap): PythonStageWrapper

  /**
   * Get the failure in PySpark, if any.
 *
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String
}

/**
 * Loader for Python transformers.
 */
private[python] object PythonStageWrapper {
  private var reader: PythonStageReader = _

  /**
   * Register Python transformer reader to load PySpark transformers.
   */
  def registerReader(r: PythonStageReader): Unit = {
    reader = r
  }

  /**
   * Load a Python transformer given path and its class name.
   */
  def load(path: String, clazz: String): PythonStageWrapper = {
    require(reader != null, "Python reader has not been registered.")
    callLoadFromPython(path, clazz)
  }

  private def callLoadFromPython(path: String, clazz: String): PythonStageWrapper = {
    val result = reader.load(path, clazz)
    val failure = reader.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    result
  }
}

/**
 * Reader to load a Python transformer. Its implementation is in PySpark.
 * See pyspark.ml.util.TransformerWrapperReader
 */
private[python] trait PythonStageReader {

  /**
   * Get the failure in PySpark, if any.
 *
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String

  def load(path: String, clazz: String): PythonStageWrapper
}

/**
 * Serializer for a Python transformer. Its implementation is in Pyspark.
 * See pyspark.ml.util.TransformerWrapperSerializer
 */
private[python] trait PythonStageSerializer {

  def dumps(id: String): Array[Byte]

  def loads(bytes: Array[Byte]): PythonStageWrapper

  /**
   * Get the failure in PySpark, if any.
 *
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String
}

/**
 * Helpers for PythonTransformerWrapperSerializer.
 */
private[python] object PythonStageSerializer {

  /**
   * A serializer in Python, used to serialize PythonTransformerWrapper.
    */
  private var serializer: PythonStageSerializer = _

  /*
   * Register a serializer from Python, should be called during initialization
   */
  def register(ser: PythonStageSerializer): Unit = synchronized {
    serializer = ser
  }

  def serialize(wrapper: PythonStageWrapper): Array[Byte] = synchronized {
    require(serializer != null, "Serializer has not been registered!")
    // get the id of PythonTransformFunction in py4j
    val h = Proxy.getInvocationHandler(wrapper.asInstanceOf[Proxy])
    val f = h.getClass.getDeclaredField("id")
    f.setAccessible(true)
    val id = f.get(h).asInstanceOf[String]
    val results = serializer.dumps(id)
    val failure = serializer.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    results
  }

  def deserialize(bytes: Array[Byte]): PythonStageWrapper = synchronized {
    require(serializer != null, "Serializer has not been registered!")
    val wrapper = serializer.loads(bytes)
    val failure = serializer.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    wrapper
  }
}

class PythonEstimator(@transient private var proxy: PythonStageWrapper)
  extends Estimator[PythonModel] with PythonStageBase {

  override val uid: String = proxy.getUid

  protected override def getProxy = this.proxy

  override def fit(dataset: Dataset[_]): PythonModel = {
    val modelWrapper = callFromPython(proxy.fit(dataset))
    new PythonModel(modelWrapper)
  }

  override def copy(extra: ParamMap): Estimator[PythonModel] = {
    this.proxy = callFromPython(proxy.copy(extra))
    this
  }

  override def transformSchema(schema: StructType): StructType = {
    callFromPython(proxy.transformSchema(schema))
  }
}

class PythonModel(@transient private var proxy: PythonStageWrapper)
  extends Model[PythonModel] with PythonStageBase {

  override val uid: String = proxy.getUid

  protected override def getProxy = this.proxy

  override def copy(extra: ParamMap): PythonModel = {
    this.proxy = callFromPython(proxy.copy(extra))
    this
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    callFromPython(proxy.transform(dataset))
  }

  override def transformSchema(schema: StructType): StructType = {
    callFromPython(proxy.transformSchema(schema))
  }
}

/**
 * A proxy transformer for all PySpark transformers implemented in pure Python.
 *
 * @param proxy A PythonTransformerWrapper which is implemented in PySpark.
 */
class PythonTransformer(@transient private var proxy: PythonStageWrapper)
  extends Transformer with PythonStageBase with MLWritable {

  override val uid: String = callFromPython(proxy.getUid)

  protected override def getProxy = this.proxy

  override def transformSchema(schema: StructType): StructType = {
    callFromPython(proxy.transformSchema(schema))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    callFromPython(proxy.transform(dataset))
  }

  /**
   * Get transformer's fully qualified class name in PySpark.
   */
  private[python] def getPythonClassName: String = {
    callFromPython(proxy.getClassName)
  }

  override def copy(extra: ParamMap): PythonTransformer = {
    this.proxy = callFromPython(proxy.copy(extra))
    this
  }

  override def write: MLWriter = new PythonTransformer.PythonTransformerWriter(this)

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val bytes = PythonStageSerializer.serialize(proxy)
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    proxy = PythonStageSerializer.deserialize(bytes)
  }
}

/**
 * Implementations of save/load for PythonTransformer.
 */
object PythonTransformer extends MLReadable[PythonTransformer] {

  override def read: MLReader[PythonTransformer] = new PythonTransformerReader

  override def load(path: String): PythonTransformer = super.load(path)

  private[python] class PythonTransformerWriter(instance: PythonTransformer) extends MLWriter {
    override def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "pyClass" -> instance.getPythonClassName
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val pyDir = new Path(path, "pyTransformer").toString
      instance.proxy.save(pyDir)
      val failure = instance.proxy.getLastFailure
      if (failure != null) {
        throw new SparkException("An exception was raised by Python:\n" + failure)
      }
    }
  }

  private class PythonTransformerReader extends MLReader[PythonTransformer] {
    private val className = classOf[PythonTransformer].getName
    override def load(path: String): PythonTransformer = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val pyClass = (metadata.metadata \ "pyClass").extract[String]
      val pyDir = new Path(path, "pyTransformer").toString
      val proxy = PythonStageWrapper.load(pyDir, pyClass)
      new PythonTransformer(proxy)
    }
  }
}

trait PythonStageBase {
  protected def getProxy: PythonStageWrapper

  protected def callFromPython[R](result: R): R = {
    val failure = getProxy.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    result
  }

  /**
   * Get serialized Python transformer
   */
  private[python] def getPythonStage: Array[Byte] = {
    callFromPython(getProxy.getStage)
  }
}
/**
 * Helper function due to Py4J error of reader/serializer does not exist in the JVM.
 */
private[python] object PythonPipelineStage {
  def registerReader(r: PythonStageReader): Unit = {
    PythonStageWrapper.registerReader(r)
  }

  def registerSerializer(ser: PythonStageSerializer): Unit = {
    PythonStageSerializer.register(ser)
  }
}
