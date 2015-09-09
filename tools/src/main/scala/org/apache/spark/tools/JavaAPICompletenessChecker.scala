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

package org.apache.spark.tools

import java.lang.reflect.{Type, Method}

import scala.collection.mutable.ArrayBuffer
import scala.language.existentials

import org.apache.spark._
import org.apache.spark.api.java._
import org.apache.spark.rdd.{RDD, DoubleRDDFunctions, PairRDDFunctions, OrderedRDDFunctions}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaPairDStream, JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, PairDStreamFunctions}


private[spark] abstract class SparkType(val name: String)

private[spark] case class BaseType(override val name: String) extends SparkType(name) {
  override def toString: String = {
    name
  }
}

private[spark]
case class ParameterizedType(override val name: String,
                             parameters: Seq[SparkType],
                             typebounds: String = "") extends SparkType(name) {
  override def toString: String = {
    if (typebounds != "") {
      typebounds + " " + name + "<" + parameters.mkString(", ") + ">"
    } else {
      name + "<" + parameters.mkString(", ") + ">"
    }
  }
}

private[spark]
case class SparkMethod(name: String, returnType: SparkType, parameters: Seq[SparkType]) {
  override def toString: String = {
    returnType + " " + name + "(" + parameters.mkString(", ") + ")"
  }
}

/**
 * A tool for identifying methods that need to be ported from Scala to the Java API.
 *
 * It uses reflection to find methods in the Scala API and rewrites those methods' signatures
 * into appropriate Java equivalents.  If those equivalent methods have not been implemented in
 * the Java API, they are printed.
 */
object JavaAPICompletenessChecker {

  private def parseType(typeStr: String): SparkType = {
    if (!typeStr.contains("<")) {
      // Base types might begin with "class" or "interface", so we have to strip that off:
      BaseType(typeStr.trim.split(" ").last)
    } else if (typeStr.endsWith("[]")) {
      ParameterizedType("Array", Seq(parseType(typeStr.stripSuffix("[]"))))
    } else {
      val parts = typeStr.split("<", 2)
      val name = parts(0).trim
      assert (parts(1).last == '>')
      val parameters = parts(1).dropRight(1)
      ParameterizedType(name, parseTypeList(parameters))
    }
  }

  private def parseTypeList(typeStr: String): Seq[SparkType] = {
    val types: ArrayBuffer[SparkType] = new ArrayBuffer[SparkType]
    var stack = 0
    var token: StringBuffer = new StringBuffer()
    for (c <- typeStr.trim) {
      if (c == ',' && stack == 0) {
        types += parseType(token.toString)
        token = new StringBuffer()
      } else if (c == ' ' && stack != 0) {
        // continue
      } else {
        if (c == '<') {
          stack += 1
        } else if (c == '>') {
          stack -= 1
        }
        token.append(c)
      }
    }
    assert (stack == 0)
    if (token.toString != "") {
      types += parseType(token.toString)
    }
    types.toSeq
  }

  private def parseReturnType(typeStr: String): SparkType = {
    if (typeStr(0) == '<') {
      val parts = typeStr.drop(0).split(">", 2)
      val parsed = parseType(parts(1)).asInstanceOf[ParameterizedType]
      ParameterizedType(parsed.name, parsed.parameters, parts(0))
    } else {
      parseType(typeStr)
    }
  }

  private def toSparkMethod(method: Method): SparkMethod = {
    val returnType = parseReturnType(method.getGenericReturnType.toString)
    val name = method.getName
    val parameters = method.getGenericParameterTypes.map(t => parseType(t.toString))
    SparkMethod(name, returnType, parameters)
  }

  private def toJavaType(scalaType: SparkType, isReturnType: Boolean): SparkType = {
    val renameSubstitutions = Map(
      "scala.collection.Map" -> "java.util.Map",
      // TODO: the JavaStreamingContext API accepts Array arguments
      // instead of Lists, so this isn't a trivial translation / sub:
      "scala.collection.Seq" -> "java.util.List",
      "scala.Function2" -> "org.apache.spark.api.java.function.Function2",
      "scala.collection.Iterator" -> "java.util.Iterator",
      "scala.collection.mutable.Queue" -> "java.util.Queue",
      "double" -> "java.lang.Double"
    )
    // Keep applying the substitutions until we've reached a fixedpoint.
    def applySubs(scalaType: SparkType): SparkType = {
      scalaType match {
        case ParameterizedType(name, parameters, typebounds) =>
          name match {
            case "org.apache.spark.rdd.RDD" =>
              if (parameters(0).name == classOf[Tuple2[_, _]].getName) {
                val tupleParams =
                  parameters(0).asInstanceOf[ParameterizedType].parameters.map(applySubs)
                ParameterizedType(classOf[JavaPairRDD[_, _]].getName, tupleParams)
              } else {
                ParameterizedType(classOf[JavaRDD[_]].getName, parameters.map(applySubs))
              }
            case "org.apache.spark.streaming.dstream.DStream" =>
              if (parameters(0).name == classOf[Tuple2[_, _]].getName) {
                val tupleParams =
                  parameters(0).asInstanceOf[ParameterizedType].parameters.map(applySubs)
                ParameterizedType("org.apache.spark.streaming.api.java.JavaPairDStream",
                  tupleParams)
              } else {
                ParameterizedType("org.apache.spark.streaming.api.java.JavaDStream",
                  parameters.map(applySubs))
              }
            case "scala.Option" => {
              if (isReturnType) {
                ParameterizedType("com.google.common.base.Optional", parameters.map(applySubs))
              } else {
                applySubs(parameters(0))
              }
            }
            case "scala.Function1" =>
              val firstParamName = parameters.last.name
              if (firstParamName.startsWith("scala.collection.Traversable") ||
                firstParamName.startsWith("scala.collection.Iterator")) {
                ParameterizedType("org.apache.spark.api.java.function.FlatMapFunction",
                  Seq(parameters(0),
                    parameters.last.asInstanceOf[ParameterizedType].parameters(0)).map(applySubs))
              } else if (firstParamName == "scala.runtime.BoxedUnit") {
                ParameterizedType("org.apache.spark.api.java.function.VoidFunction",
                  parameters.dropRight(1).map(applySubs))
              } else {
                ParameterizedType("org.apache.spark.api.java.function.Function",
                  parameters.map(applySubs))
              }
            case _ =>
              ParameterizedType(renameSubstitutions.getOrElse(name, name),
                parameters.map(applySubs))
          }
        case BaseType(name) =>
          if (renameSubstitutions.contains(name)) {
            BaseType(renameSubstitutions(name))
          } else {
            scalaType
          }
      }
    }
    var oldType = scalaType
    var newType = applySubs(scalaType)
    while (oldType != newType) {
      oldType = newType
      newType = applySubs(scalaType)
    }
    newType
  }

  private def toJavaMethod(method: SparkMethod): SparkMethod = {
    val params = method.parameters
      .filterNot(_.name == "scala.reflect.ClassTag")
      .map(toJavaType(_, isReturnType = false))
    SparkMethod(method.name, toJavaType(method.returnType, isReturnType = true), params)
  }

  private def isExcludedByName(method: Method): Boolean = {
    val name = method.getDeclaringClass.getName + "." + method.getName
    // Scala methods that are declared as private[mypackage] become public in the resulting
    // Java bytecode.  As a result, we need to manually exclude those methods here.
    // This list also includes a few methods that are only used by the web UI or other
    // internal Spark components.
    val excludedNames = Seq(
      "org.apache.spark.rdd.RDD.origin",
      "org.apache.spark.rdd.RDD.elementClassTag",
      "org.apache.spark.rdd.RDD.checkpointData",
      "org.apache.spark.rdd.RDD.partitioner",
      "org.apache.spark.rdd.RDD.partitions",
      "org.apache.spark.rdd.RDD.firstParent",
      "org.apache.spark.rdd.RDD.doCheckpoint",
      "org.apache.spark.rdd.RDD.markCheckpointed",
      "org.apache.spark.rdd.RDD.clearDependencies",
      "org.apache.spark.rdd.RDD.getDependencies",
      "org.apache.spark.rdd.RDD.getPartitions",
      "org.apache.spark.rdd.RDD.dependencies",
      "org.apache.spark.rdd.RDD.getPreferredLocations",
      "org.apache.spark.rdd.RDD.collectPartitions",
      "org.apache.spark.rdd.RDD.computeOrReadCheckpoint",
      "org.apache.spark.rdd.PairRDDFunctions.getKeyClass",
      "org.apache.spark.rdd.PairRDDFunctions.getValueClass",
      "org.apache.spark.SparkContext.stringToText",
      "org.apache.spark.SparkContext.makeRDD",
      "org.apache.spark.SparkContext.runJob",
      "org.apache.spark.SparkContext.runApproximateJob",
      "org.apache.spark.SparkContext.clean",
      "org.apache.spark.SparkContext.metadataCleaner",
      "org.apache.spark.SparkContext.ui",
      "org.apache.spark.SparkContext.newShuffleId",
      "org.apache.spark.SparkContext.newRddId",
      "org.apache.spark.SparkContext.cleanup",
      "org.apache.spark.SparkContext.receiverJobThread",
      "org.apache.spark.SparkContext.getRDDStorageInfo",
      "org.apache.spark.SparkContext.addedFiles",
      "org.apache.spark.SparkContext.addedJars",
      "org.apache.spark.SparkContext.persistentRdds",
      "org.apache.spark.SparkContext.executorEnvs",
      "org.apache.spark.SparkContext.checkpointDir",
      "org.apache.spark.SparkContext.getSparkHome",
      "org.apache.spark.SparkContext.executorMemoryRequested",
      "org.apache.spark.SparkContext.getExecutorStorageStatus",
      "org.apache.spark.streaming.dstream.DStream.generatedRDDs",
      "org.apache.spark.streaming.dstream.DStream.zeroTime",
      "org.apache.spark.streaming.dstream.DStream.rememberDuration",
      "org.apache.spark.streaming.dstream.DStream.storageLevel",
      "org.apache.spark.streaming.dstream.DStream.mustCheckpoint",
      "org.apache.spark.streaming.dstream.DStream.checkpointDuration",
      "org.apache.spark.streaming.dstream.DStream.checkpointData",
      "org.apache.spark.streaming.dstream.DStream.graph",
      "org.apache.spark.streaming.dstream.DStream.isInitialized",
      "org.apache.spark.streaming.dstream.DStream.parentRememberDuration",
      "org.apache.spark.streaming.dstream.DStream.initialize",
      "org.apache.spark.streaming.dstream.DStream.validate",
      "org.apache.spark.streaming.dstream.DStream.setContext",
      "org.apache.spark.streaming.dstream.DStream.setGraph",
      "org.apache.spark.streaming.dstream.DStream.remember",
      "org.apache.spark.streaming.dstream.DStream.getOrCompute",
      "org.apache.spark.streaming.dstream.DStream.generateJob",
      "org.apache.spark.streaming.dstream.DStream.clearOldMetadata",
      "org.apache.spark.streaming.dstream.DStream.addMetadata",
      "org.apache.spark.streaming.dstream.DStream.updateCheckpointData",
      "org.apache.spark.streaming.dstream.DStream.restoreCheckpointData",
      "org.apache.spark.streaming.dstream.DStream.isTimeValid",
      "org.apache.spark.streaming.StreamingContext.nextNetworkInputStreamId",
      "org.apache.spark.streaming.StreamingContext.checkpointDir",
      "org.apache.spark.streaming.StreamingContext.checkpointDuration",
      "org.apache.spark.streaming.StreamingContext.receiverJobThread",
      "org.apache.spark.streaming.StreamingContext.scheduler",
      "org.apache.spark.streaming.StreamingContext.initialCheckpoint",
      "org.apache.spark.streaming.StreamingContext.getNewNetworkStreamId",
      "org.apache.spark.streaming.StreamingContext.validate",
      "org.apache.spark.streaming.StreamingContext.createNewSparkContext",
      "org.apache.spark.streaming.StreamingContext.rddToFileName",
      "org.apache.spark.streaming.StreamingContext.getSparkCheckpointDir",
      "org.apache.spark.streaming.StreamingContext.env",
      "org.apache.spark.streaming.StreamingContext.graph",
      "org.apache.spark.streaming.StreamingContext.isCheckpointPresent"
    )
    val excludedPatterns = Seq(
      """^org\.apache\.spark\.SparkContext\..*To.*Functions""",
      """^org\.apache\.spark\.SparkContext\..*WritableConverter""",
      """^org\.apache\.spark\.SparkContext\..*To.*Writable"""
    ).map(_.r)
    lazy val excludedByPattern =
      !excludedPatterns.map(_.findFirstIn(name)).filter(_.isDefined).isEmpty
    name.contains("$") || excludedNames.contains(name) || excludedByPattern
  }

  private def isExcludedByInterface(method: Method): Boolean = {
    val excludedInterfaces =
      Set("org.apache.spark.Logging", "org.apache.hadoop.mapreduce.HadoopMapReduceUtil")
    def toComparisionKey(method: Method): (Class[_], String, Type) =
      (method.getReturnType, method.getName, method.getGenericReturnType)
    val interfaces = method.getDeclaringClass.getInterfaces.filter { i =>
      excludedInterfaces.contains(i.getName)
    }
    val excludedMethods = interfaces.flatMap(_.getMethods.map(toComparisionKey))
    excludedMethods.contains(toComparisionKey(method))
  }

  private def printMissingMethods(scalaClass: Class[_], javaClass: Class[_]) {
    val methods = scalaClass.getMethods
      .filterNot(_.isAccessible)
      .filterNot(isExcludedByName)
      .filterNot(isExcludedByInterface)
    val javaEquivalents = methods.map(m => toJavaMethod(toSparkMethod(m))).toSet

    val javaMethods = javaClass.getMethods.map(toSparkMethod).toSet

    val missingMethods = javaEquivalents -- javaMethods

    for (method <- missingMethods) {
      // scalastyle:off println
      println(method)
      // scalastyle:on println
    }
  }

  def main(args: Array[String]) {
    // scalastyle:off println
    println("Missing RDD methods")
    printMissingMethods(classOf[RDD[_]], classOf[JavaRDD[_]])
    println()

    println("Missing PairRDD methods")
    printMissingMethods(classOf[PairRDDFunctions[_, _]], classOf[JavaPairRDD[_, _]])
    println()

    println("Missing DoubleRDD methods")
    printMissingMethods(classOf[DoubleRDDFunctions], classOf[JavaDoubleRDD])
    println()

    println("Missing OrderedRDD methods")
    printMissingMethods(classOf[OrderedRDDFunctions[_, _, _]], classOf[JavaPairRDD[_, _]])
    println()

    println("Missing SparkContext methods")
    printMissingMethods(classOf[SparkContext], classOf[JavaSparkContext])
    println()

    println("Missing StreamingContext methods")
    printMissingMethods(classOf[StreamingContext], classOf[JavaStreamingContext])
    println()

    println("Missing DStream methods")
    printMissingMethods(classOf[DStream[_]], classOf[JavaDStream[_]])
    println()

    println("Missing PairDStream methods")
    printMissingMethods(classOf[PairDStreamFunctions[_, _]], classOf[JavaPairDStream[_, _]])
    println()
    // scalastyle:on println
  }
}
