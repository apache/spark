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

import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.{Vectors, Vector}

import org.codehaus.janino.ClassBodyEvaluator
/**
 * An object for creating a code generated decision tree model.
 * NodeToTree is used to convert a node to a series if code gen
 * if/else statements conditions returning the predicition for a
 * given vector.
 * getScorer wraps this and provides a function we can use to get
 * the prediction.
 */
private[spark] object CodeGenerationDecisionTreeModel extends Logging {
  private val prefix = "mllibCodeGen"
  private val curId = new java.util.concurrent.atomic.AtomicInteger()

  /**
   * Compile the Java source code into a Java class, using Janino.
   * Based on Spark SQL's implementation. This should be moved to a common class
   * once we have multiple code generators in ML.
   *
   * It will track the time used to compile
   */
  protected def compile(code: String, implements: Array[Class[_]]): Class[_] = {
    val startTime = System.nanoTime()
    val evaluator = new ClassBodyEvaluator()
    val clName = freshName()
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setImplementedInterfaces(implements)
    evaluator.setClassName(clName)
    evaluator.setDefaultImports(Array(
      "org.apache.spark.mllib.linalg.Vectors",
      "org.apache.spark.mllib.linalg.Vector"
    ))
    evaluator.cook(s"${clName}.java", code)
    val clazz = evaluator.getClazz()
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
   * series of if/else statements. If the tree is too large to fit in a single
   * in-line method breaks it up into multiple methods.
   * Returns a string for the current function body and a string of any additional
   * functions.
   */
  def nodeToTree(root: Node, depth: Int): (String, String) = {
    // Handle the different types of nodes
    root match {
      case node: InternalNode => {
        // Handle trees that get too large to fit in a single in-line java method
        depth match {
          case 8 => {
            val newFunctionName = freshName()
            val newFunction = nodeToFunction(root, newFunctionName)
            (s"return ${newFunctionName}(input);", newFunction)
          }
          case _ => {
            val nodeSplit = node.split
            val (leftSubCode, leftSubFunction) = nodeToTree(node.leftChild, depth + 1)
            val (rightSubCode, rightSubFunction) = nodeToTree(node.rightChild, depth + 1)
            val subCode = nodeSplit match {
              case split: CategoricalSplit => {
                val isLeft = split.isLeft
                isLeft match {
                  case true => s"""
                              if (categories.contains(input.apply(${split.featureIndex}))) {
                                ${leftSubCode}
                              } else {
                                ${rightSubCode}
                              }"""
                  case false => s"""
                               if (categories.contains(input.apply(${split.featureIndex}))) {
                                 ${leftSubCode}
                               } else {
                                 ${rightSubCode}
                               }"""
                }
              }
              case split: ContinuousSplit => {
                s"""
               if (input.apply(${split.featureIndex}) <= ${split.threshold}) {
                 ${leftSubCode}
                } else {
                 ${rightSubCode}
                }"""
              }
            }
            (subCode, leftSubFunction + rightSubFunction)
          }
        }
      }
      case node: LeafNode => (s"return ${node.prediction};", "")
    }
  }

  /**
   * Convert the tree starting at the provided root node into a code generated
   * series of if/else statements. If the tree is too large to fit in a single
   * in-line method breaks it up into multiple methods.
   */
  def nodeToFunction(node: Node, name: String): String = {
    val (code, extraFunctions) = nodeToTree(node, 0)
    s"""
     public double ${name}(Vector input) throws Exception {
       ${code}
     }

     ${extraFunctions}
     """
  }

  // Create a codegened scorer for a given node
  def getScorer(root: Node): Vector => Double = {
    val code =
      s"""
       @Override
       ${nodeToFunction(root, "call")}
       """
    trait CallableVectorDouble {
      def call(v: Vector): Double
    }
    val jfunc = compile(code,
      Array(classOf[Serializable], classOf[CallableVectorDouble])).newInstance()
    def func(v: Vector): Double = {
      jfunc.asInstanceOf[CallableVectorDouble].call(v)
    }
    func(_)
  }
}
