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

package org.apache.spark.ml.util

import java.io.IOException
import java.util.{Locale, ServiceLoader}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.annotation.{Since, Unstable}
import org.apache.spark.internal.Logging
import org.apache.spark.ml._
import org.apache.spark.ml.classification.{OneVsRest, OneVsRestModel}
import org.apache.spark.ml.feature.RFormulaModel
import org.apache.spark.ml.param.{ParamPair, Params}
import org.apache.spark.ml.tuning.ValidatorParams
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.util.{Utils, VersionUtils}

/**
 * Trait for `MLWriter` and `MLReader`.
 */
private[util] sealed trait BaseReadWrite {
  private var optionSparkSession: Option[SparkSession] = None

  /**
   * Sets the Spark Session to use for saving/loading.
   */
  @Since("2.0.0")
  def session(sparkSession: SparkSession): this.type = {
    optionSparkSession = Option(sparkSession)
    this
  }

  /**
   * Returns the user-specified Spark Session or the default.
   */
  protected final def sparkSession: SparkSession = {
    if (optionSparkSession.isEmpty) {
      optionSparkSession = Some(SparkSession.builder().getOrCreate())
    }
    optionSparkSession.get
  }

  /**
   * Returns the user-specified SQL context or the default.
   */
  protected final def sqlContext: SQLContext = sparkSession.sqlContext

  /** Returns the underlying `SparkContext`. */
  protected final def sc: SparkContext = sparkSession.sparkContext
}

/**
 * Abstract class to be implemented by objects that provide ML exportability.
 *
 * A new instance of this class will be instantiated each time a save call is made.
 *
 * Must have a valid zero argument constructor which will be called to instantiate.
 *
 * @since 2.4.0
 */
@Unstable
@Since("2.4.0")
trait MLWriterFormat {
  /**
   * Function to write the provided pipeline stage out.
   *
   * @param path  The path to write the result out to.
   * @param session  SparkSession associated with the write request.
   * @param optionMap  User provided options stored as strings.
   * @param stage  The pipeline stage to be saved.
   */
  @Since("2.4.0")
  def write(path: String, session: SparkSession, optionMap: mutable.Map[String, String],
    stage: PipelineStage): Unit
}

/**
 * ML export formats for should implement this trait so that users can specify a shortname rather
 * than the fully qualified class name of the exporter.
 *
 * A new instance of this class will be instantiated each time a save call is made.
 *
 * @since 2.4.0
 */
@Unstable
@Since("2.4.0")
trait MLFormatRegister extends MLWriterFormat {
  /**
   * The string that represents the format that this format provider uses. This is, along with
   * stageName, is overridden by children to provide a nice alias for the writer. For example:
   *
   * {{{
   *   override def format(): String =
   *       "pmml"
   * }}}
   * Indicates that this format is capable of saving a pmml model.
   *
   * Must have a valid zero argument constructor which will be called to instantiate.
   *
   * Format discovery is done using a ServiceLoader so make sure to list your format in
   * META-INF/services.
   * @since 2.4.0
   */
  @Since("2.4.0")
  def format(): String

  /**
   * The string that represents the stage type that this writer supports. This is, along with
   * format, is overridden by children to provide a nice alias for the writer. For example:
   *
   * {{{
   *   override def stageName(): String =
   *       "org.apache.spark.ml.regression.LinearRegressionModel"
   * }}}
   * Indicates that this format is capable of saving Spark's own PMML model.
   *
   * Format discovery is done using a ServiceLoader so make sure to list your format in
   * META-INF/services.
   * @since 2.4.0
   */
  @Since("2.4.0")
  def stageName(): String

  private[ml] def shortName(): String = s"${format()}+${stageName()}"
}

/**
 * Abstract class for utility classes that can save ML instances in Spark's internal format.
 */
@Since("1.6.0")
abstract class MLWriter extends BaseReadWrite with Logging {

  protected var shouldOverwrite: Boolean = false

  /**
   * Saves the ML instances to the input path.
   */
  @Since("1.6.0")
  @throws[IOException]("If the input path already exists but overwrite is not enabled.")
  def save(path: String): Unit = {
    new FileSystemOverwrite().handleOverwrite(path, shouldOverwrite, sparkSession)
    saveImpl(path)
  }

  /**
   * `save()` handles overwriting and then calls this method.  Subclasses should override this
   * method to implement the actual saving of the instance.
   */
  @Since("1.6.0")
  protected def saveImpl(path: String): Unit

  /**
   * Overwrites if the output path already exists.
   */
  @Since("1.6.0")
  def overwrite(): this.type = {
    shouldOverwrite = true
    this
  }

  /**
   * Map to store extra options for this writer.
   */
  protected val optionMap: mutable.Map[String, String] = new mutable.HashMap[String, String]()

  /**
   * Adds an option to the underlying MLWriter. See the documentation for the specific model's
   * writer for possible options. The option name (key) is case-insensitive.
   */
  @Since("2.3.0")
  def option(key: String, value: String): this.type = {
    require(key != null && !key.isEmpty)
    optionMap.put(key.toLowerCase(Locale.ROOT), value)
    this
  }

  // override for Java compatibility
  @Since("1.6.0")
  override def session(sparkSession: SparkSession): this.type = super.session(sparkSession)
}

/**
 * A ML Writer which delegates based on the requested format.
 */
@Unstable
@Since("2.4.0")
class GeneralMLWriter(stage: PipelineStage) extends MLWriter with Logging {
  private var source: String = "internal"

  /**
   * Specifies the format of ML export (e.g. "pmml", "internal", or
   * the fully qualified class name for export).
   */
  @Since("2.4.0")
  def format(source: String): this.type = {
    this.source = source
    this
  }

  /**
   * Dispatches the save to the correct MLFormat.
   */
  @Since("2.4.0")
  @throws[IOException]("If the input path already exists but overwrite is not enabled.")
  @throws[SparkException]("If multiple sources for a given short name format are found.")
  override protected def saveImpl(path: String): Unit = {
    val loader = Utils.getContextOrSparkClassLoader
    val serviceLoader = ServiceLoader.load(classOf[MLFormatRegister], loader)
    val stageName = stage.getClass.getName
    val targetName = s"$source+$stageName"
    val formats = serviceLoader.asScala.toList
    val shortNames = formats.map(_.shortName())
    val writerCls = formats.filter(_.shortName().equalsIgnoreCase(targetName)) match {
      // requested name did not match any given registered alias
      case Nil =>
        Try(loader.loadClass(source)) match {
          case Success(writer) =>
            // Found the ML writer using the fully qualified path
            writer
          case Failure(error) =>
            throw new SparkException(
              s"Could not load requested format $source for $stageName ($targetName) had $formats" +
              s"supporting $shortNames", error)
        }
      case head :: Nil =>
        head.getClass
      case _ =>
        // Multiple sources
        throw new SparkException(
          s"Multiple writers found for $source+$stageName, try using the class name of the writer")
    }
    if (classOf[MLWriterFormat].isAssignableFrom(writerCls)) {
      val writer = writerCls.getConstructor().newInstance().asInstanceOf[MLWriterFormat]
      writer.write(path, sparkSession, optionMap, stage)
    } else {
      throw new SparkException(s"ML source $source is not a valid MLWriterFormat")
    }
  }

  // override for Java compatibility
  override def session(sparkSession: SparkSession): this.type = super.session(sparkSession)
}

/**
 * Trait for classes that provide `MLWriter`.
 */
@Since("1.6.0")
trait MLWritable {

  /**
   * Returns an `MLWriter` instance for this ML instance.
   */
  @Since("1.6.0")
  def write: MLWriter

  /**
   * Saves this ML instance to the input path, a shortcut of `write.save(path)`.
   */
  @Since("1.6.0")
  @throws[IOException]("If the input path already exists but overwrite is not enabled.")
  def save(path: String): Unit = write.save(path)
}

/**
 * Trait for classes that provide `GeneralMLWriter`.
 */
@Since("2.4.0")
@Unstable
trait GeneralMLWritable extends MLWritable {
  /**
   * Returns an `MLWriter` instance for this ML instance.
   */
  @Since("2.4.0")
  override def write: GeneralMLWriter
}

/**
 * Helper trait for making simple `Params` types writable.  If a `Params` class stores
 * all data as [[org.apache.spark.ml.param.Param]] values, then extending this trait will provide
 * a default implementation of writing saved instances of the class.
 * This only handles simple [[org.apache.spark.ml.param.Param]] types; e.g., it will not handle
 * [[org.apache.spark.sql.Dataset]].
 *
 * @see `DefaultParamsReadable`, the counterpart to this trait
 */
trait DefaultParamsWritable extends MLWritable { self: Params =>

  override def write: MLWriter = new DefaultParamsWriter(this)
}

/**
 * Abstract class for utility classes that can load ML instances.
 *
 * @tparam T ML instance type
 */
@Since("1.6.0")
abstract class MLReader[T] extends BaseReadWrite {

  /**
   * Loads the ML component from the input path.
   */
  @Since("1.6.0")
  def load(path: String): T

  // override for Java compatibility
  override def session(sparkSession: SparkSession): this.type = super.session(sparkSession)
}

/**
 * Trait for objects that provide `MLReader`.
 *
 * @tparam T ML instance type
 */
@Since("1.6.0")
trait MLReadable[T] {

  /**
   * Returns an `MLReader` instance for this class.
   */
  @Since("1.6.0")
  def read: MLReader[T]

  /**
   * Reads an ML instance from the input path, a shortcut of `read.load(path)`.
   *
   * @note Implementing classes should override this to be Java-friendly.
   */
  @Since("1.6.0")
  def load(path: String): T = read.load(path)
}


/**
 * Helper trait for making simple `Params` types readable.  If a `Params` class stores
 * all data as [[org.apache.spark.ml.param.Param]] values, then extending this trait will provide
 * a default implementation of reading saved instances of the class.
 * This only handles simple [[org.apache.spark.ml.param.Param]] types; e.g., it will not handle
 * [[org.apache.spark.sql.Dataset]].
 *
 * @tparam T ML instance type
 * @see `DefaultParamsWritable`, the counterpart to this trait
 */
trait DefaultParamsReadable[T] extends MLReadable[T] {

  override def read: MLReader[T] = new DefaultParamsReader[T]
}

/**
 * Default `MLWriter` implementation for transformers and estimators that contain basic
 * (json4s-serializable) params and no data. This will not handle more complex params or types with
 * data (e.g., models with coefficients).
 *
 * @param instance object to save
 */
private[ml] class DefaultParamsWriter(instance: Params) extends MLWriter {

  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
  }
}

private[ml] object DefaultParamsWriter {

  /**
   * Saves metadata + Params to: path + "/metadata"
   *  - class
   *  - timestamp
   *  - sparkVersion
   *  - uid
   *  - defaultParamMap
   *  - paramMap
   *  - (optionally, extra metadata)
   *
   * @param extraMetadata  Extra metadata to be saved at same level as uid, paramMap, etc.
   * @param paramMap  If given, this is saved in the "paramMap" field.
   *                  Otherwise, all [[org.apache.spark.ml.param.Param]]s are encoded using
   *                  [[org.apache.spark.ml.param.Param.jsonEncode()]].
   */
  def saveMetadata(
      instance: Params,
      path: String,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None,
      paramMap: Option[JValue] = None): Unit = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataJson = getMetadataToSave(instance, sc, extraMetadata, paramMap)
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

  /**
   * Helper for [[saveMetadata()]] which extracts the JSON to save.
   * This is useful for ensemble models which need to save metadata for many sub-models.
   *
   * @see [[saveMetadata()]] for details on what this includes.
   */
  def getMetadataToSave(
      instance: Params,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None,
      paramMap: Option[JValue] = None): String = {
    val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.paramMap.toSeq
    val defaultParams = instance.defaultParamMap.toSeq
    val jsonParams = paramMap.getOrElse(render(params.map { case ParamPair(p, v) =>
      p.name -> parse(p.jsonEncode(v))
    }.toList))
    val jsonDefaultParams = render(defaultParams.map { case ParamPair(p, v) =>
      p.name -> parse(p.jsonEncode(v))
    }.toList)
    val basicMetadata = ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams) ~
      ("defaultParamMap" -> jsonDefaultParams)
    val metadata = extraMetadata match {
      case Some(jObject) =>
        basicMetadata ~ jObject
      case None =>
        basicMetadata
    }
    val metadataJson: String = compact(render(metadata))
    metadataJson
  }
}

/**
 * Default `MLReader` implementation for transformers and estimators that contain basic
 * (json4s-serializable) params and no data. This will not handle more complex params or types with
 * data (e.g., models with coefficients).
 *
 * @tparam T ML instance type
 * TODO: Consider adding check for correct class name.
 */
private[ml] class DefaultParamsReader[T] extends MLReader[T] {

  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    val instance =
      cls.getConstructor(classOf[String]).newInstance(metadata.uid).asInstanceOf[Params]
    metadata.getAndSetParams(instance)
    instance.asInstanceOf[T]
  }
}

private[ml] object DefaultParamsReader {

  /**
   * All info from metadata file.
   *
   * @param params  paramMap, as a `JValue`
   * @param defaultParams defaultParamMap, as a `JValue`. For metadata file prior to Spark 2.4,
   *                      this is `JNothing`.
   * @param metadata  All metadata, including the other fields
   * @param metadataJson  Full metadata file String (for debugging)
   */
  case class Metadata(
      className: String,
      uid: String,
      timestamp: Long,
      sparkVersion: String,
      params: JValue,
      defaultParams: JValue,
      metadata: JValue,
      metadataJson: String) {


    private def getValueFromParams(params: JValue): Seq[(String, JValue)] = {
      params match {
        case JObject(pairs) => pairs
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot recognize JSON metadata: $metadataJson.")
      }
    }

    /**
     * Get the JSON value of the [[org.apache.spark.ml.param.Param]] of the given name.
     * This can be useful for getting a Param value before an instance of `Params`
     * is available. This will look up `params` first, if not existing then looking up
     * `defaultParams`.
     */
    def getParamValue(paramName: String): JValue = {
      implicit val format = DefaultFormats

      // Looking up for `params` first.
      var pairs = getValueFromParams(params)
      var foundPairs = pairs.filter { case (pName, jsonValue) =>
        pName == paramName
      }
      if (foundPairs.length == 0) {
        // Looking up for `defaultParams` then.
        pairs = getValueFromParams(defaultParams)
        foundPairs = pairs.filter { case (pName, jsonValue) =>
          pName == paramName
        }
      }
      assert(foundPairs.length == 1, s"Expected one instance of Param '$paramName' but found" +
        s" ${foundPairs.length} in JSON Params: " + pairs.map(_.toString).mkString(", "))

      foundPairs.map(_._2).head
    }

    /**
     * Extract Params from metadata, and set them in the instance.
     * This works if all Params (except params included by `skipParams` list) implement
     * [[org.apache.spark.ml.param.Param.jsonDecode()]].
     *
     * @param skipParams The params included in `skipParams` won't be set. This is useful if some
     *                   params don't implement [[org.apache.spark.ml.param.Param.jsonDecode()]]
     *                   and need special handling.
     */
    def getAndSetParams(
        instance: Params,
        skipParams: Option[List[String]] = None): Unit = {
      setParams(instance, skipParams, isDefault = false)

      // For metadata file prior to Spark 2.4, there is no default section.
      val (major, minor) = VersionUtils.majorMinorVersion(sparkVersion)
      if (major > 2 || (major == 2 && minor >= 4)) {
        setParams(instance, skipParams, isDefault = true)
      }
    }

    private def setParams(
        instance: Params,
        skipParams: Option[List[String]],
        isDefault: Boolean): Unit = {
      implicit val format = DefaultFormats
      val paramsToSet = if (isDefault) defaultParams else params
      paramsToSet match {
        case JObject(pairs) =>
          pairs.foreach { case (paramName, jsonValue) =>
            if (skipParams == None || !skipParams.get.contains(paramName)) {
              val param = instance.getParam(paramName)
              val value = param.jsonDecode(compact(render(jsonValue)))
              if (isDefault) {
                Params.setDefault(instance, param, value)
              } else {
                instance.set(param, value)
              }
            }
          }
        case _ =>
          throw new IllegalArgumentException(
            s"Cannot recognize JSON metadata: ${metadataJson}.")
      }
    }
  }

  /**
   * Load metadata saved using [[DefaultParamsWriter.saveMetadata()]]
   *
   * @param expectedClassName  If non empty, this is checked against the loaded metadata.
   * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
   */
  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = ""): Metadata = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sc.textFile(metadataPath, 1).first()
    parseMetadata(metadataStr, expectedClassName)
  }

  /**
   * Parse metadata JSON string produced by [[DefaultParamsWriter.getMetadataToSave()]].
   * This is a helper function for [[loadMetadata()]].
   *
   * @param metadataStr  JSON string of metadata
   * @param expectedClassName  If non empty, this is checked against the loaded metadata.
   * @throws IllegalArgumentException if expectedClassName is specified and does not match metadata
   */
  def parseMetadata(metadataStr: String, expectedClassName: String = ""): Metadata = {
    val metadata = parse(metadataStr)

    implicit val format = DefaultFormats
    val className = (metadata \ "class").extract[String]
    val uid = (metadata \ "uid").extract[String]
    val timestamp = (metadata \ "timestamp").extract[Long]
    val sparkVersion = (metadata \ "sparkVersion").extract[String]
    val defaultParams = metadata \ "defaultParamMap"
    val params = metadata \ "paramMap"
    if (expectedClassName.nonEmpty) {
      require(className == expectedClassName, s"Error loading metadata: Expected class name" +
        s" $expectedClassName but found class name $className")
    }

    Metadata(className, uid, timestamp, sparkVersion, params, defaultParams, metadata, metadataStr)
  }

  /**
   * Load a `Params` instance from the given path, and return it.
   * This assumes the instance implements [[MLReadable]].
   */
  def loadParamsInstance[T](path: String, sc: SparkContext): T =
    loadParamsInstanceReader(path, sc).load(path)

  /**
   * Load a `Params` instance reader from the given path, and return it.
   * This assumes the instance implements [[MLReadable]].
   */
  def loadParamsInstanceReader[T](path: String, sc: SparkContext): MLReader[T] = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    val cls = Utils.classForName(metadata.className)
    cls.getMethod("read").invoke(null).asInstanceOf[MLReader[T]]
  }
}

/**
 * Default Meta-Algorithm read and write implementation.
 */
private[ml] object MetaAlgorithmReadWrite {
  /**
   * Examine the given estimator (which may be a compound estimator) and extract a mapping
   * from UIDs to corresponding `Params` instances.
   */
  def getUidMap(instance: Params): Map[String, Params] = {
    val uidList = getUidMapImpl(instance)
    val uidMap = uidList.toMap
    if (uidList.size != uidMap.size) {
      throw new RuntimeException(s"${instance.getClass.getName}.load found a compound estimator" +
        s" with stages with duplicate UIDs. List of UIDs: ${uidList.map(_._1).mkString(", ")}.")
    }
    uidMap
  }

  private def getUidMapImpl(instance: Params): List[(String, Params)] = {
    val subStages: Array[Params] = instance match {
      case p: Pipeline => p.getStages.asInstanceOf[Array[Params]]
      case pm: PipelineModel => pm.stages.asInstanceOf[Array[Params]]
      case v: ValidatorParams => Array(v.getEstimator, v.getEvaluator)
      case ovr: OneVsRest => Array(ovr.getClassifier)
      case ovrModel: OneVsRestModel => Array(ovrModel.getClassifier) ++ ovrModel.models
      case rformModel: RFormulaModel => Array(rformModel.pipelineModel)
      case _: Params => Array.empty[Params]
    }
    val subStageMaps = subStages.flatMap(getUidMapImpl)
    List((instance.uid, instance)) ++ subStageMaps
  }
}

private[ml] class FileSystemOverwrite extends Logging {

  def handleOverwrite(path: String, shouldOverwrite: Boolean, session: SparkSession): Unit = {
    val hadoopConf = session.sessionState.newHadoopConf()
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    if (fs.exists(qualifiedOutputPath)) {
      if (shouldOverwrite) {
        logInfo(s"Path $path already exists. It will be overwritten.")
        // TODO: Revert back to the original content if save is not successful.
        fs.delete(qualifiedOutputPath, true)
      } else {
        throw new IOException(s"Path $path already exists. To overwrite it, " +
          s"please use write.overwrite().save(path) for Scala and use " +
          s"write().overwrite().save(path) for Java and Python.")
      }
    }
  }
}
