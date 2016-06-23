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

import scala.reflect._
import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path
import org.json4s._

import org.apache.spark.SparkException
import org.apache.spark.ml.{Estimator, Model, PipelineStage, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Wrapper of PipelineStage (Estimator/Model/Transformer) written in pure Python, which
 * implementation is in PySpark. See pyspark.ml.util.StageWrapper
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
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String
}

/**
 * ML Reader for Python PipelineStages. The implementation of the reader is in Python, which is
 * registered here the moment we creating a new PythonStageWrapper.
 */
private[python] object PythonStageWrapper {
  private var reader: PythonStageReader = _

  /**
   * Register Python stage reader to load PySpark PipelineStages.
   */
  def registerReader(r: PythonStageReader): Unit = {
    reader = r
  }

  /**
   * Load a Python PipelineStage given its path and class name.
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
 * Reader to load a pure Python PipelineStage. Its implementation is in PySpark.
 * See pyspark.ml.util.StageReader
 */
private[python] trait PythonStageReader {

  def getLastFailure: String

  def load(path: String, clazz: String): PythonStageWrapper
}

/**
 * Serializer of a pure Python PipelineStage. Its implementation is in Pyspark.
 * See pyspark.ml.util.StageSerializer
 */
private[python] trait PythonStageSerializer {

  def dumps(id: String): Array[Byte]

  def loads(bytes: Array[Byte]): PythonStageWrapper

  def getLastFailure: String
}

/**
 * Helpers for PythonStageSerializer.
 */
private[python] object PythonStageSerializer {

  /**
   * A serializer in Python, used to serialize PythonStageWrapper.
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

/**
 * A proxy estimator for all PySpark estimator written in pure Python.
 */
class PythonEstimator(@transient private var proxy: PythonStageWrapper)
  extends Estimator[PythonModel] with PythonStageBase with MLWritable {

  override val uid: String = proxy.getUid

  private[python] override def getProxy = this.proxy

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

  override def write: MLWriter = new PythonEstimator.PythonEstimatorWriter(this)

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    proxy = PythonStageSerializer.deserialize(bytes)
  }
}

object PythonEstimator extends MLReadable[PythonEstimator] {

  override def read: MLReader[PythonEstimator] = new PythonEstimatorReader

  override def load(path: String): PythonEstimator = super.load(path)

  private[python] class PythonEstimatorWriter(instance: PythonEstimator)
    extends PythonStage.Writer[PythonEstimator](instance)

  private class PythonEstimatorReader extends PythonStage.Reader[PythonEstimator]
}

/**
 * A proxy model of all PySpark Model written in pure Python.
 */
class PythonModel(@transient private var proxy: PythonStageWrapper)
  extends Model[PythonModel] with PythonStageBase with MLWritable {

  override val uid: String = proxy.getUid

  private[python] override def getProxy = this.proxy

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

  override def write: MLWriter = new PythonModel.PythonModelWriter(this)

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    proxy = PythonStageSerializer.deserialize(bytes)
  }
}

object PythonModel extends MLReadable[PythonModel] {

  override def read: MLReader[PythonModel] = new PythonModelReader

  override def load(path: String): PythonModel = super.load(path)

  private[python] class PythonModelWriter(instance: PythonModel)
    extends PythonStage.Writer[PythonModel](instance)

  private class PythonModelReader extends PythonStage.Reader[PythonModel]
}

/**
 * A proxy transformer for all PySpark transformers written in pure Python.
 */
class PythonTransformer(@transient private var proxy: PythonStageWrapper)
  extends Transformer with PythonStageBase with MLWritable {

  override val uid: String = callFromPython(proxy.getUid)

  private[python] override def getProxy = this.proxy

  override def transformSchema(schema: StructType): StructType = {
    callFromPython(proxy.transformSchema(schema))
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    callFromPython(proxy.transform(dataset))
  }

  override def copy(extra: ParamMap): PythonTransformer = {
    this.proxy = callFromPython(proxy.copy(extra))
    this
  }

  override def write: MLWriter = new PythonTransformer.PythonTransformerWriter(this)

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    proxy = PythonStageSerializer.deserialize(bytes)
  }
}

object PythonTransformer extends MLReadable[PythonTransformer] {

  override def read: MLReader[PythonTransformer] = new PythonTransformerReader

  override def load(path: String): PythonTransformer = super.load(path)

  private[python] class PythonTransformerWriter(instance: PythonTransformer)
    extends PythonStage.Writer[PythonTransformer](instance)

  private class PythonTransformerReader extends PythonStage.Reader[PythonTransformer]
}

/**
 * Common functions for Python PipelineStage.
 */
trait PythonStageBase {

  private[python] def getProxy: PythonStageWrapper

  private[python] def callFromPython[R](result: R): R = {
    val failure = getProxy.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Python:\n" + failure)
    }
    result
  }

  /**
   * Get serialized Python PipelineStage.
   */
  private[python] def getPythonStage: Array[Byte] = {
    callFromPython(getProxy.getStage)
  }

  /**
   * Get the stage's fully qualified class name in PySpark.
   */
  private[python] def getPythonClassName: String = {
    callFromPython(getProxy.getClassName)
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val bytes = PythonStageSerializer.serialize(getProxy)
    out.writeInt(bytes.length)
    out.write(bytes)
  }
}

private[python] object PythonStage {
  /**
   * Helper functions due to Py4J error of reader/serializer does not exist in the JVM.
   */
  def registerReader(r: PythonStageReader): Unit = {
    PythonStageWrapper.registerReader(r)
  }

  def registerSerializer(ser: PythonStageSerializer): Unit = {
    PythonStageSerializer.register(ser)
  }

  /**
   * Helper functions for Reader/Writer in Python Stages.
   */
  private[python] class Writer[S <: PipelineStage with PythonStageBase](instance: S)
    extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "pyClass" -> instance.getPythonClassName
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val pyDir = new Path(path, s"pyStage-${instance.uid}").toString
      instance.callFromPython(instance.getProxy.save(pyDir))
    }
  }

  private[python] class Reader[S <: PipelineStage with PythonStageBase: ClassTag]
    extends MLReader[S] {
    private val className = classTag[S].runtimeClass.getName
    override def load(path: String): S = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val pyClass = (metadata.metadata \ "pyClass").extract[String]
      val pyDir = new Path(path, s"pyStage-${metadata.uid}").toString
      val proxy = PythonStageWrapper.load(pyDir, pyClass)
      classTag[S].runtimeClass.getConstructor(classOf[PythonStageWrapper])
        .newInstance(proxy).asInstanceOf[S]
    }
  }
}
