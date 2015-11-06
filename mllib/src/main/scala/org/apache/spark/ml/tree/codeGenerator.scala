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

package org.apache.spark.ml.tree

import org.apache.spark.mllib.linalg.{Vectors, Vector}

import org.codehaus.janino.ClassBodyEvaluatoin

/**
 * An object for creating a code generated decision tree model.
 * NodeToTree is used to convert a node to a series if code gen
 * if/else statements conditions returning the predicition for a
 * given vector.
 * getScorer wraps this and provides a function we can use to get
 * the prediction.
 */
private[spark] object CodeGenerationDecisionTreeModel with logging {
  private val prefix = "mllibCodeGen"
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Compile the Java source code into a Java class, using Janino.
   * Based on Spark SQL's implementation. This should be moved to a common class
   * once we have multiple code generators in ML.
   *
   * It will track the time used to compile
   */
  protected def compile(code: String): Class[_] = {
    val startTime = System.nanoTime()
    val clazz = new ClassBodyEvaluator(code).getClazz()
    val endTime = System.nanoTime()
    def timeMs: Double = (endTime - startTime).toDouble / 1000000
    logDebug(s"Compiled Java code (${code.size} bytes) in $timeMs ms")
    clazz
  }

  protected def freshName(): String = {
    s"$prefix${curId.getAndIncrement}"
  }


  /**
   * Convert the tree starting at the provided root node into a code generated
   * series of if/else statements.
   */
  def NodeToTree(root: Node): String = {
    root match {
      case node: InternalNode => {
        val nodeSplit = node.split
        nodeSplit match {
          case split: CategoricalSplit => {
            val isLeft = split.isLeft
            isLeft match {
              case true => s"""
                              if (categories.contains(input.get(${split.featureIndex}))) {
                                ${NodeToTree(node.leftChild)};
                              } else {
                                ${NodeToTree(node.rightChild)};
                              }"""
              case false => s"""
                               if (categories.contains(input.get(${split.featureIndex}))) {
                                 ${NodeToTree(node.leftChild)};
                               } else {
                                 ${NodeToTree(node.rightChild)};
                               }"""
            }
          }
          case split: ContinuousSplit => {
            s"""
               if (input.get(${split.featureIndex}) <= ${split.threshold}) {
                 ${NodeToTree(node.leftChild)};
                } else {
                 ${NodeToTree(node.rightChild)};
                }"""
          }
        }
      }
      case node: LeafNode => q"return ${node.prediction};"
    }
  }

  // Create a codegened scorer for a given node
  def getScorer(root: Node): Vector => Double = {
    val toolbox = currentMirror.mkToolBox()
    val code =
      s"""
         import org.apache.spark.mllib.linalg.Vectors;
         import org.apache.spark.mllib.linalg.Vector;
         import org.apache.spark.api.java.function;
         class ${freshName() implements Function<Vector, Double> {
            public Double call(Vector root) {
              ${NodeToTree(root)}
            }
         };
       """
    val jfunc = toolbox.eval(code).asInstanceOf[Function[Vector, Double]]
    (x: Vector => jfunc.call(x))
  }
}
