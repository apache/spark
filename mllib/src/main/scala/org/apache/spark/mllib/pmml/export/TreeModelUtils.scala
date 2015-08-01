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

package org.apache.spark.mllib.pmml.export

import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}
import org.dmg.pmml.{Node => PMMLNode, _}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

private[mllib] object TreeModelUtils {


  def getPMMLTree(mllibTreeModel: DecisionTreeModel, modelName: String): TreeModel = {

    val miningFunctionType = getPMMLMiningFunctionType(mllibTreeModel.algo)

    val miningSchema = new MiningSchema()
    .withMiningFields(getMiningFieldsForTree(mllibTreeModel).asJava)

    val treeModel = new org.dmg.pmml.TreeModel()
    .withModelName(modelName)
    .withFunctionName(miningFunctionType)
    .withNode(buildStub(mllibTreeModel.topNode))
    .withMiningSchema(miningSchema)

    treeModel

  }

  /** Build a pmml tree stub given the root mllib node. */
  private def buildStub(rootNode: Node): PMMLNode = {
    // get rootPMML node for the MLLib node
    val rootPMMLNode = createNode(rootNode)

    if (!rootNode.isLeaf) {
      // if left node exist, add the node
      if (rootNode.leftNode.isDefined) {
        val leftNode = buildStub(rootNode.leftNode.get)
        rootPMMLNode.withNodes(leftNode)
      }
      // if right node exist, add the node
      if (rootNode.rightNode.isDefined) {
        val rightNode = buildStub(rootNode.rightNode.get)
        rootPMMLNode.withNodes(rightNode)
      }
    }

    rootPMMLNode
  }

  /** Get pmml Predicate for a given mlLib tree node. */
  private def getPredicate(node: Node): Option[Predicate] = {
    // compound predicate if classification and categories list length > 0

    if (node.split.isDefined) {
      val split = node.split.get
      val featureType = split.featureType
      val fieldName = FieldName.create("field_" + split.feature.toString)
      featureType match {
        case FeatureType.Continuous => {
          val value = split.threshold.toString
          Some(
            new SimplePredicate(fieldName, SimplePredicate.Operator.LESS_OR_EQUAL)
            .withValue(value)
          )
        }
        case FeatureType.Categorical => {
          if (split.categories.length > 1) {
            val predicates: List[Predicate] =
              for (category <- split.categories)
                yield
                new SimplePredicate(fieldName, SimplePredicate.Operator.EQUAL)
                .withValue(category.toString)

            val compoundPredicate = new CompoundPredicate()
            .withBooleanOperator(CompoundPredicate.BooleanOperator.OR)
            .withPredicates(predicates.asJava)

            Some(compoundPredicate)

          }
          else {
            val value = split.categories(0).toString
            Some(new SimplePredicate(fieldName, SimplePredicate.Operator.EQUAL).withValue(value))
          }
        }
      }
    }
    else
      None
  }

  /** Create equivalent PMML node for given mlLib node. */
  private def createNode(mlLibNode: Node): PMMLNode = {
    val node = new PMMLNode()
    .withId(mlLibNode.id.toString)
    .withScore(mlLibNode.predict.predict.toString)

    val predicate = getPredicate(mlLibNode)
    if (predicate.isDefined) {
      node.withPredicate(predicate.get)
    }

    node
  }

  /** Get PMML mining function type for given mlLib Algo. */
  private def getPMMLMiningFunctionType(mlLibAlgo: Algo): MiningFunctionType = {
    mlLibAlgo match {
      case Algo.Classification => MiningFunctionType.CLASSIFICATION
      case Algo.Regression => MiningFunctionType.REGRESSION
    }
  }

  /** Get PMML datafield based on the mllib split feature. */
  private def getDataField(mllibNode: Node): Option[DataField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get
      val dataField = new DataField()
      .withName(FieldName.create("field_" + split.feature.toString))
      .withDataType(DataType.fromValue(split.threshold.getClass.getSimpleName.toLowerCase))
      .withOpType(OpType.fromValue(split.featureType.toString.toLowerCase))

      split.featureType match {
        case FeatureType.Continuous => dataField.withOpType(OpType.CONTINUOUS)
        case FeatureType.Categorical => {
          dataField.withOpType(OpType.CATEGORICAL)
          val categories = split.categories
          .map(category => new org.dmg.pmml.Value(category.toString)).asJava
          dataField.withValues(categories)
        }
      }

      Some(dataField)
    }
    else {
      None
    }
  }

  /** Get PMML Mining field based on Mllib node split feature. */
  private def getMiningField(mllibNode: Node): Option[MiningField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get

      val miningField = new MiningField()
      .withName(FieldName.create("field_" + split.feature.toString))
      .withUsageType(FieldUsageType.ACTIVE)

      Some(miningField)
    }
    else{
      None
    }

  }

  /** Get distinct PMML mining fields list for a given mlLib decision tree model. */
  def getMiningFieldsForTree(treeModel: DecisionTreeModel): List[MiningField] = {

    @tailrec
    def getMiningFieldsForTreeRec(
      nodeList: List[Node],
      miningFlds: List[MiningField]): List[MiningField] = {

      nodeList match {
        case Nil => miningFlds
        case nd :: ls if (nd.isLeaf) => miningFlds
        case nd :: ls if (!nd.isLeaf && nd.split.isDefined) => {
          val ndList = MutableList[Node]()
          if (nd.leftNode.isDefined){
            ndList += nd.leftNode.get
          }

          if (nd.rightNode.isDefined) {
            ndList += nd.rightNode.get
          }

          getMiningFieldsForTreeRec(ndList.toList, miningFlds :+ getMiningField(nd).get)

        }
      }
    }

    val miningFields = getMiningFieldsForTreeRec(List(treeModel.topNode), List[MiningField]())
    // the miningfields collected from the different nodes could have duplicate.
    // get the distinct fields.
    val distinctFields = miningFields
    .groupBy(dField => dField.getName.getValue)
    .map { case (name, fields) => fields(0) }

    distinctFields.toList
  }

  /** Get distince PMML datafields list for a given mlllib decision tree model. */
  def getDataFieldsForTree(treeModel: DecisionTreeModel): List[DataField] = {

    @tailrec
    def getDataFieldsForTreeRec(
      nodeList: List[Node],
      dataFlds: List[DataField]): List[DataField] = {

      nodeList match {
        case Nil => dataFlds
        case nd :: ls if (nd.isLeaf) => dataFlds
        case nd :: ls if (!nd.isLeaf && nd.split.isDefined) => {
          val ndList = MutableList[Node]()
          if (nd.leftNode.isDefined) {
            ndList += nd.leftNode.get
          }
          if (nd.rightNode.isDefined) {
            ndList += nd.rightNode.get
          }

          getDataFieldsForTreeRec(ndList.toList, dataFlds :+ getDataField(nd).get)

        }
      }
    }
    val dataFields = getDataFieldsForTreeRec(List(treeModel.topNode), List[DataField]())

    // There could be duplicate fields in the datafields collected above if 2 splits use the same
    // features
    val distinctFields = dataFields.groupBy(dataField => dataField.getName.getValue).map {
      case (fieldName, dataFieldsArr) => {

        val dataField: DataField = dataFieldsArr.head.getOpType match {
          // in case of continuous optype/feature type get one data field out of the duplicates
          case OpType.CONTINUOUS => dataFieldsArr.head
          // for categorical feature type, different splits might use the same feature but
          // different or same categories
          // so need to collect distinct values for the categories too
          case OpType.CATEGORICAL => dataFieldsArr.toList.reduce((a, b) => {
            val values = (a.getValues.asScala.toList ++ b.getValues.asScala.toList).groupBy(v =>
              v.getValue)
            val distinctValues = values.map { case (string, valueList) => valueList(0) }.toList
            .asJava

            new DataField().withName(a.getName)
            .withOpType(a.getOpType)
            .withValues(distinctValues)
          })

          case _ => throw new RuntimeException("Only continuous and catigorical datatypes are " +
          "supported now.")
        }
        dataField
      }
    }

    distinctFields.toList
  }
}
