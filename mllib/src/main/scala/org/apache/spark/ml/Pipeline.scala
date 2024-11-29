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

package org.apache.spark.ml

import java.{util => ju}

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * A stage in a pipeline, either an [[Estimator]] or a [[Transformer]].
 */
abstract class PipelineStage extends Params with Logging {

  /**
   *
   * Check transform validity and derive the output schema from the input schema.
   *
   * We check validity for interactions between parameters during `transformSchema` and
   * raise an exception if any parameter value is invalid. Parameter value checks which
   * do not depend on other parameters are handled by `Param.validate()`.
   *
   * Typical implementation should first conduct verification on schema change and parameter
   * validity, including complex parameter interaction checks.
   */
  def transformSchema(schema: StructType): StructType

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema and parameters, optionally with logging.
   *
   * This should be optimistic.  If it is unclear whether the schema will be valid, then it should
   * be assumed valid until proven otherwise.
   */
  @DeveloperApi
  protected def transformSchema(
      schema: StructType,
      logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }

  override def copy(extra: ParamMap): PipelineStage
}

/**
 * A simple pipeline, which acts as an estimator. A Pipeline consists of a sequence of stages, each
 * of which is either an [[Estimator]] or a [[Transformer]]. When `Pipeline.fit` is called, the
 * stages are executed in order. If a stage is an [[Estimator]], its `Estimator.fit` method will
 * be called on the input dataset to fit a model. Then the model, which is a transformer, will be
 * used to transform the dataset as the input to the next stage. If a stage is a [[Transformer]],
 * its `Transformer.transform` method will be called to produce the dataset for the next stage.
 * The fitted model from a [[Pipeline]] is a [[PipelineModel]], which consists of fitted models and
 * transformers, corresponding to the pipeline stages. If there are no stages, the pipeline acts as
 * an identity transformer.
 */
@Since("1.2.0")
class Pipeline @Since("1.4.0") (
  @Since("1.4.0") override val uid: String) extends Estimator[PipelineModel] with MLWritable {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("pipeline"))

  /**
   * param for pipeline stages
   * @group param
   */
  @Since("1.2.0")
  val stages: Param[Array[PipelineStage]] = new Param(this, "stages", "stages of the pipeline")

  /** @group setParam */
  @Since("1.2.0")
  def setStages(value: Array[_ <: PipelineStage]): this.type = {
    set(stages, value.asInstanceOf[Array[PipelineStage]])
    this
  }

  // Below, we clone stages so that modifications to the list of stages will not change
  // the Param value in the Pipeline.
  /** @group getParam */
  @Since("1.2.0")
  def getStages: Array[PipelineStage] = $(stages).clone()

  /**
   * Fits the pipeline to the input dataset with additional parameters. If a stage is an
   * [[Estimator]], its `Estimator.fit` method will be called on the input dataset to fit a model.
   * Then the model, which is a transformer, will be used to transform the dataset as the input to
   * the next stage. If a stage is a [[Transformer]], its `Transformer.transform` method will be
   * called to produce the dataset for the next stage. The fitted model from a [[Pipeline]] is an
   * [[PipelineModel]], which consists of fitted models and transformers, corresponding to the
   * pipeline stages. If there are no stages, the output model acts as an identity transformer.
   *
   * @param dataset input dataset
   * @return fitted pipeline
   */
  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): PipelineModel = instrumented(
      instr => instr.withFitEvent(this, dataset) {
    transformSchema(dataset.schema, logging = true)
    val theStages = $(stages)
    // Search for the last estimator.
    var indexOfLastEstimator = -1
    theStages.iterator.zipWithIndex.foreach { case (stage, index) =>
      stage match {
        case _: Estimator[_] =>
          indexOfLastEstimator = index
        case _ =>
      }
    }
    var curDataset = dataset
    val transformers = ListBuffer.empty[Transformer]
    theStages.iterator.zipWithIndex.foreach { case (stage, index) =>
      if (index <= indexOfLastEstimator) {
        val transformer = stage match {
          case estimator: Estimator[_] =>
            instr.withFitEvent(estimator, curDataset)(estimator.fit(curDataset))
          case t: Transformer =>
            t
          case _ =>
            throw new IllegalArgumentException(
              s"Does not support stage $stage of type ${stage.getClass}")
        }
        if (index < indexOfLastEstimator) {
          curDataset = instr.withTransformEvent(
            transformer, curDataset)(transformer.transform(curDataset))
        }
        transformers += transformer
      } else {
        transformers += stage.asInstanceOf[Transformer]
      }
    }

    new PipelineModel(uid, transformers.toArray).setParent(this)
  })

  @Since("1.4.0")
  override def copy(extra: ParamMap): Pipeline = {
    val map = extractParamMap(extra)
    val newStages = map(stages).map(_.copy(extra))
    new Pipeline(uid).setStages(newStages)
  }

  @Since("1.2.0")
  override def transformSchema(schema: StructType): StructType = {
    val theStages = $(stages)
    require(theStages.toSet.size == theStages.length,
      "Cannot have duplicate components in a pipeline.")
    theStages.foldLeft(schema)((cur, stage) => stage.transformSchema(cur))
  }

  @Since("1.6.0")
  override def write: MLWriter = new Pipeline.PipelineWriter(this)
}

@Since("1.6.0")
object Pipeline extends MLReadable[Pipeline] {

  @Since("1.6.0")
  override def read: MLReader[Pipeline] = new PipelineReader

  @Since("1.6.0")
  override def load(path: String): Pipeline = super.load(path)

  private[Pipeline] class PipelineWriter(val instance: Pipeline) extends MLWriter {

    SharedReadWrite.validateStages(instance.getStages)

    override def save(path: String): Unit =
      instrumented(_.withSaveInstanceEvent(this, path)(super.save(path)))
    override protected def saveImpl(path: String): Unit =
      SharedReadWrite.saveImpl(instance, instance.getStages, sparkSession, path)
  }

  private class PipelineReader extends MLReader[Pipeline] {

    /** Checked against metadata when loading model */
    private val className = classOf[Pipeline].getName

    override def load(path: String): Pipeline = instrumented(_.withLoadInstanceEvent(this, path) {
      val (uid: String, stages: Array[PipelineStage]) =
        SharedReadWrite.load(className, sparkSession, path)
      new Pipeline(uid).setStages(stages)
    })
  }

  /**
   * Methods for `MLReader` and `MLWriter` shared between [[Pipeline]] and [[PipelineModel]]
   */
  private[ml] object SharedReadWrite {

    import org.json4s.JsonDSL._

    /** Check that all stages are Writable */
    def validateStages(stages: Array[PipelineStage]): Unit = {
      stages.foreach {
        case stage: MLWritable => // good
        case other =>
          throw new UnsupportedOperationException("Pipeline write will fail on this Pipeline" +
            s" because it contains a stage which does not implement Writable. Non-Writable stage:" +
            s" ${other.uid} of type ${other.getClass}")
      }
    }

    /**
     * Save metadata and stages for a [[Pipeline]] or [[PipelineModel]]
     *  - save metadata to path/metadata
     *  - save stages to stages/IDX_UID
     */
    @deprecated("use saveImpl with SparkSession", "4.0.0")
    def saveImpl(
        instance: Params,
        stages: Array[PipelineStage],
        sc: SparkContext,
        path: String): Unit =
      saveImpl(
        instance,
        stages,
        SparkSession.builder().sparkContext(sc).getOrCreate(),
        path)

    def saveImpl(
        instance: Params,
        stages: Array[PipelineStage],
        spark: SparkSession,
        path: String): Unit = instrumented { instr =>
      val stageUids = stages.map(_.uid)
      val jsonParams = List("stageUids" -> parse(compact(render(stageUids.toImmutableArraySeq))))
      DefaultParamsWriter.saveMetadata(instance, path, spark, None, Some(jsonParams))

      // Save stages
      val stagesDir = new Path(path, "stages").toString
      stages.zipWithIndex.foreach { case (stage, idx) =>
        val writer = stage.asInstanceOf[MLWritable].write
        val stagePath = getStagePath(stage.uid, idx, stages.length, stagesDir)
        instr.withSaveInstanceEvent(writer, stagePath)(writer.save(stagePath))
      }
    }

    /**
     * Load metadata and stages for a [[Pipeline]] or [[PipelineModel]]
     * @return  (UID, list of stages)
     */
    @deprecated("use load with SparkSession", "4.0.0")
    def load(
        expectedClassName: String,
        sc: SparkContext,
        path: String): (String, Array[PipelineStage]) =
      load(
        expectedClassName,
        SparkSession.builder().sparkContext(sc).getOrCreate(),
        path)

    def load(
        expectedClassName: String,
        spark: SparkSession,
        path: String): (String, Array[PipelineStage]) = instrumented { instr =>
      val metadata = DefaultParamsReader.loadMetadata(path, spark, expectedClassName)

      implicit val format = DefaultFormats
      val stagesDir = new Path(path, "stages").toString
      val stageUids: Array[String] = (metadata.params \ "stageUids").extract[Seq[String]].toArray
      val stages: Array[PipelineStage] = stageUids.zipWithIndex.map { case (stageUid, idx) =>
        val stagePath = SharedReadWrite.getStagePath(stageUid, idx, stageUids.length, stagesDir)
        val reader = DefaultParamsReader.loadParamsInstanceReader[PipelineStage](stagePath, spark)
        instr.withLoadInstanceEvent(reader, stagePath)(reader.load(stagePath))
      }
      (metadata.uid, stages)
    }

    /** Get path for saving the given stage. */
    def getStagePath(stageUid: String, stageIdx: Int, numStages: Int, stagesDir: String): String = {
      val stageIdxDigits = numStages.toString.length
      val idxFormat = s"%0${stageIdxDigits}d"
      val stageDir = idxFormat.format(stageIdx) + "_" + stageUid
      new Path(stagesDir, stageDir).toString
    }
  }
}

/**
 * Represents a fitted pipeline.
 */
@Since("1.2.0")
class PipelineModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("1.4.0") val stages: Array[Transformer])
  extends Model[PipelineModel] with MLWritable with Logging {

  /** A Java/Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, stages: ju.List[Transformer]) = {
    this(uid, stages.asScala.toArray)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = instrumented(instr =>
      instr.withTransformEvent(this, dataset) {
    transformSchema(dataset.schema, logging = true)
    stages.foldLeft(dataset.toDF())((cur, transformer) =>
      instr.withTransformEvent(transformer, cur)(transformer.transform(cur)))
  })

  @Since("1.2.0")
  override def transformSchema(schema: StructType): StructType = {
    stages.foldLeft(schema)((cur, transformer) => transformer.transformSchema(cur))
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): PipelineModel = {
    new PipelineModel(uid, stages.map(_.copy(extra))).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new PipelineModel.PipelineModelWriter(this)
}

@Since("1.6.0")
object PipelineModel extends MLReadable[PipelineModel] {

  import Pipeline.SharedReadWrite

  @Since("1.6.0")
  override def read: MLReader[PipelineModel] = new PipelineModelReader

  @Since("1.6.0")
  override def load(path: String): PipelineModel = super.load(path)

  private[PipelineModel] class PipelineModelWriter(val instance: PipelineModel) extends MLWriter {

    SharedReadWrite.validateStages(instance.stages.asInstanceOf[Array[PipelineStage]])

    override def save(path: String): Unit =
      instrumented(_.withSaveInstanceEvent(this, path)(super.save(path)))
    override protected def saveImpl(path: String): Unit = SharedReadWrite.saveImpl(instance,
      instance.stages.asInstanceOf[Array[PipelineStage]], sparkSession, path)
  }

  private class PipelineModelReader extends MLReader[PipelineModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[PipelineModel].getName

    override def load(path: String): PipelineModel = instrumented(_.withLoadInstanceEvent(
        this, path) {
      val (uid: String, stages: Array[PipelineStage]) =
        SharedReadWrite.load(className, sparkSession, path)
      val transformers = stages map {
        case stage: Transformer => stage
        case other => throw new RuntimeException(s"PipelineModel.read loaded a stage but found it" +
          s" was not a Transformer.  Bad stage ${other.uid} of type ${other.getClass}")
      }
      new PipelineModel(uid, transformers)
    })
  }
}
