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

import org.apache.spark.annotation.Since
import org.apache.spark.connect.proto
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


// TODO: Using Cleaner interface to clean server side object.
case class ModelRef(refId: String)

object ModelRef {
  def fromProto(protoValue: proto.ModelRef): ModelRef = {
    ModelRef(protoValue.getId)
  }

  def toProto(modelRef: ModelRef): proto.ModelRef = {
    proto.ModelRef.newBuilder().setId(modelRef.refId).build()
  }
}


/**
 * A fitted model, i.e., a [[Transformer]] produced by an [[Estimator]].
 *
 * @tparam M
 *   model type
 */
abstract class Model[M <: Model[M]] extends Transformer {

  @transient var modelRef: ModelRef = _

  /**
   * The parent estimator that produced this model.
   * @note
   *   For ensembles' component Models, this value can be null.
   */
  @transient var parent: Estimator[M] = _

  /**
   * Sets the parent of this model (Java API).
   */
  @Since("3.5.0")
  def setParent(parent: Estimator[M]): M = {
    this.parent = parent
    this.asInstanceOf[M]
  }

  /** Indicates whether this [[Model]] has a corresponding parent. */
  @Since("3.5.0")
  def hasParent: Boolean = parent != null

  @Since("3.5.0")
  override def copy(extra: ParamMap): M = {
    val cmdBuilder = proto.MlCommand.newBuilder()

    cmdBuilder.getCopyModelBuilder
      .setModelRef(ModelRef.toProto(modelRef))

    val resp = SparkSession.active.executeMl(cmdBuilder.build())
    val newRef = ConnectUtils.deserializeResponseValue(resp).asInstanceOf[ModelRef]
    val newModel = defaultCopy(extra).asInstanceOf[M]
    newModel.modelRef = newRef
    newModel
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    dataset.sparkSession.newDataFrame { builder =>
      builder.getMlRelationBuilder.getModelTransformBuilder
        .setInput(dataset.plan.getRoot)
        .setModelRef(ModelRef.toProto(modelRef))
        .setParams(ConnectUtils.getInstanceParamsProto(this))
    }
  }

  protected def getModelAttr(name: String): Any = {
    val cmdBuilder = proto.MlCommand.newBuilder()

    cmdBuilder.getFetchModelAttrBuilder
      .setModelRef(ModelRef.toProto(modelRef))
      .setName(name)

    val resp = SparkSession.active.executeMl(cmdBuilder.build())
    ConnectUtils.deserializeResponseValue(resp)
  }

}

trait ModelSummary {

  protected def model: Model[_]

  protected def datasetOpt: Option[Dataset[_]]

  protected def getModelSummaryAttr(name: String): Any = {
    val cmdBuilder = proto.MlCommand.newBuilder()

    val fetchCmdBuilder = cmdBuilder.getFetchModelSummaryAttrBuilder

    fetchCmdBuilder
      .setModelRef(ModelRef.toProto(model.modelRef))
      .setName(name)
      .setParams(ConnectUtils.getInstanceParamsProto(model))

    datasetOpt.map(x => fetchCmdBuilder.setEvaluationDataset(x.plan.getRoot))

    val resp = SparkSession.active.executeMl(cmdBuilder.build())
    ConnectUtils.deserializeResponseValue(resp)
  }

  protected def getModelSummaryAttrDataFrame(name: String): DataFrame = {
    SparkSession.active.newDataFrame { builder =>
      builder.getMlRelationBuilder.getModelSummaryAttrBuilder
        .setName(name)
        .setModelRef(ModelRef.toProto(model.modelRef))
        .setParams(ConnectUtils.getInstanceParamsProto(model))
      datasetOpt.map { x =>
        builder.getMlRelationBuilder.getModelSummaryAttrBuilder.setEvaluationDataset(
          x.plan.getRoot
        )
      }
    }
  }
}
