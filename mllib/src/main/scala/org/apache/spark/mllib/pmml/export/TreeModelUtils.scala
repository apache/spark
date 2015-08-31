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

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

import org.dmg.pmml.{Node => PMMLNode, Value => PMMLValue, _}

import org.apache.spark.mllib.tree.configuration.{Algo, FeatureType}
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.model.{DecisionTreeModel, Node}

private[mllib] object TreeModelUtils {

  val FieldNamePrefix = "field_"


  def toPMMLTree(dtModel: DecisionTreeModel, modelName: String): (TreeModel, List[DataField]) = {

    val miningFunctionType = getPMMLMiningFunctionType(dtModel.algo)

    val treeModel = new org.dmg.pmml.TreeModel()
      .withModelName(modelName)
      .withFunctionName(miningFunctionType)
      .withSplitCharacteristic(TreeModel.SplitCharacteristic.BINARY_SPLIT)

    val (rootNode, miningFields, dataFields) = buildStub(dtModel.topNode)

    val miningSchema = new MiningSchema()
      .withMiningFields(miningFields.asJava)


    treeModel.withNode(rootNode)
      .withMiningSchema(miningSchema)

    (treeModel, dataFields)

  }

  /** Build a pmml tree stub given the root mllib node. */
  private def buildStub(rootDTNode: Node): (PMMLNode,List[MiningField], List[DataField]) = {

    val miningFields = MutableList[MiningField]()
    val dataFields = mutable.HashMap[String,DataField]()

    def buildStubInternal(rootNode: Node): PMMLNode = {

      // get rootPMML node for the MLLib node
      val rootPMMLNode = createNode(rootNode)

      if (!rootNode.isLeaf) {

        if(rootNode.split.isDefined){
          val fieldName = FieldName.create(FieldNamePrefix + rootNode.split.get.feature)
          val dataField = getDataField(rootNode, fieldName).get

          if(! dataFields.get(dataField.getName.getValue).isDefined){
            dataFields.put(dataField.getName.getValue, dataField)
            miningFields += new MiningField()
              .withName(dataField.getName)
              .withUsageType(FieldUsageType.ACTIVE)

          }else if(dataField.getOpType != OpType.CONTINUOUS.value()){
            appendCategories(
              dataFields.get(dataField.getName.getValue).get,
              dataField.getValues.asScala.toList)
          }

          rootPMMLNode.withPredicate(getPredicate(rootNode, Some(dataField.getName)))

        }
        // if left node exist, add the node
        if (rootNode.leftNode.isDefined) {
          val leftNode = buildStubInternal(rootNode.leftNode.get)
          rootPMMLNode.withNodes(leftNode)
        }
        // if right node exist, add the node
        if (rootNode.rightNode.isDefined) {
          val rightNode = buildStubInternal(rootNode.rightNode.get)
          rootPMMLNode.withNodes(rightNode)
        }
      }else{
        rootPMMLNode.withPredicate(getPredicate(rootNode, None))
      }
      rootPMMLNode
    }

    val pmmlTreeRootNode = buildStubInternal(rootDTNode)

    (pmmlTreeRootNode, miningFields.toList, dataFields.values.toList)

  }


  private def appendCategories(dtField: DataField, values:List[PMMLValue]): DataField = {
    if(dtField.getOpType == OpType.CATEGORICAL){

      val existingValues = dtField.getValues.asScala
        .groupBy{case category=> category.getValue}.toMap

      values.foreach(category => {
        if(!existingValues.get(category.getValue).isDefined){
          dtField.withValues(category)
        }
      })
    }

    dtField
  }

  /** Get pmml Predicate for a given mlLib tree node. */
  private def getPredicate(node: Node, fieldName: Option[FieldName]): Predicate = {
    // compound predicate if classification and categories list length > 0

    if (node.split.isDefined) {

      require(fieldName.isDefined, "fieldName should not be None, it should be defined.")
      val field = fieldName.get

      val split = node.split.get
      val featureType = split.featureType

      featureType match {
        case FeatureType.Continuous => {
          val value = split.threshold.toString
          new SimplePredicate(field, SimplePredicate.Operator.LESS_OR_EQUAL)
            .withValue(value)
        }
        case FeatureType.Categorical => {
          if (split.categories.length > 1) {
            val predicates: List[Predicate] =
              for (category <- split.categories)
                yield
                new SimplePredicate(field, SimplePredicate.Operator.EQUAL)
                  .withValue(category.toString)

            val compoundPredicate = new CompoundPredicate()
              .withBooleanOperator(CompoundPredicate.BooleanOperator.OR)
              .withPredicates(predicates.asJava)

            compoundPredicate

          }
          else {
            val value = split.categories(0).toString
            new SimplePredicate(field, SimplePredicate.Operator.EQUAL).withValue(value)
          }
        }
      }
    }
    else {
      new True()
    }

  }

  /** Create equivalent PMML node for given mlLib node. */
  private def createNode(mlLibNode: Node): PMMLNode = {
    val node = new PMMLNode()
      .withId(mlLibNode.id.toString)
      .withScore(mlLibNode.predict.predict.toString)

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
  private def getDataField(mllibNode: Node, fieldName: FieldName): Option[DataField] = {
    if (!mllibNode.isLeaf && mllibNode.split.isDefined) {
      val split = mllibNode.split.get
      val dataField = new DataField()
        .withName(fieldName)
        .withDataType(DataType.fromValue(split.threshold.getClass.getSimpleName.toLowerCase))
        .withOpType(OpType.fromValue(split.featureType.toString.toLowerCase))

      split.featureType match {
        case FeatureType.Continuous => dataField.withOpType(OpType.CONTINUOUS)
        case FeatureType.Categorical => {
          dataField.withOpType(OpType.CATEGORICAL)
          val categories = split.categories
            .map(category => new PMMLValue(category.toString)).asJava
          dataField.withValues(categories)
        }
      }

      Some(dataField)
    }
    else {
      None
    }
  }
}
