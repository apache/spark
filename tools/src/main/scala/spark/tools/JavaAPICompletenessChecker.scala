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

package spark.tools

import java.lang.reflect.Method

import scala.collection.mutable.ArrayBuffer

import spark._
import spark.api.java._
import spark.rdd.OrderedRDDFunctions
import spark.streaming.{PairDStreamFunctions, DStream, StreamingContext}
import spark.streaming.api.java.{JavaPairDStream, JavaDStream, JavaStreamingContext}


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
      "scala.Function2" -> "spark.api.java.function.Function2",
      "scala.collection.Iterator" -> "java.util.Iterator",
      "scala.collection.mutable.Queue" -> "java.util.Queue",
      "double" -> "java.lang.Double"
    )
    // Keep applying the substitutions until we've reached a fixedpoint.
    def applySubs(scalaType: SparkType): SparkType = {
      scalaType match {
        case ParameterizedType(name, parameters, typebounds) =>
          name match {
            case "spark.RDD" =>
              if (parameters(0).name == classOf[Tuple2[_, _]].getName) {
                val tupleParams =
                  parameters(0).asInstanceOf[ParameterizedType].parameters.map(applySubs)
                ParameterizedType(classOf[JavaPairRDD[_, _]].getName, tupleParams)
              } else {
                ParameterizedType(classOf[JavaRDD[_]].getName, parameters.map(applySubs))
              }
            case "spark.streaming.DStream" =>
              if (parameters(0).name == classOf[Tuple2[_, _]].getName) {
                val tupleParams =
                  parameters(0).asInstanceOf[ParameterizedType].parameters.map(applySubs)
                ParameterizedType("spark.streaming.api.java.JavaPairDStream", tupleParams)
              } else {
                ParameterizedType("spark.streaming.api.java.JavaDStream",
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
                ParameterizedType("spark.api.java.function.FlatMapFunction",
                  Seq(parameters(0),
                    parameters.last.asInstanceOf[ParameterizedType].parameters(0)).map(applySubs))
              } else if (firstParamName == "scala.runtime.BoxedUnit") {
                ParameterizedType("spark.api.java.function.VoidFunction",
                  parameters.dropRight(1).map(applySubs))
              } else {
                ParameterizedType("spark.api.java.function.Function", parameters.map(applySubs))
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
      .filterNot(_.name == "scala.reflect.ClassManifest")
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
      "spark.RDD.origin",
      "spark.RDD.elementClassManifest",
      "spark.RDD.checkpointData",
      "spark.RDD.partitioner",
      "spark.RDD.partitions",
      "spark.RDD.firstParent",
      "spark.RDD.doCheckpoint",
      "spark.RDD.markCheckpointed",
      "spark.RDD.clearDependencies",
      "spark.RDD.getDependencies",
      "spark.RDD.getPartitions",
      "spark.RDD.dependencies",
      "spark.RDD.getPreferredLocations",
      "spark.RDD.collectPartitions",
      "spark.RDD.computeOrReadCheckpoint",
      "spark.PairRDDFunctions.getKeyClass",
      "spark.PairRDDFunctions.getValueClass",
      "spark.SparkContext.stringToText",
      "spark.SparkContext.makeRDD",
      "spark.SparkContext.runJob",
      "spark.SparkContext.runApproximateJob",
      "spark.SparkContext.clean",
      "spark.SparkContext.metadataCleaner",
      "spark.SparkContext.ui",
      "spark.SparkContext.newShuffleId",
      "spark.SparkContext.newRddId",
      "spark.SparkContext.cleanup",
      "spark.SparkContext.receiverJobThread",
      "spark.SparkContext.getRDDStorageInfo",
      "spark.SparkContext.addedFiles",
      "spark.SparkContext.addedJars",
      "spark.SparkContext.persistentRdds",
      "spark.SparkContext.executorEnvs",
      "spark.SparkContext.checkpointDir",
      "spark.SparkContext.getSparkHome",
      "spark.SparkContext.executorMemoryRequested",
      "spark.SparkContext.getExecutorStorageStatus",
      "spark.streaming.DStream.generatedRDDs",
      "spark.streaming.DStream.zeroTime",
      "spark.streaming.DStream.rememberDuration",
      "spark.streaming.DStream.storageLevel",
      "spark.streaming.DStream.mustCheckpoint",
      "spark.streaming.DStream.checkpointDuration",
      "spark.streaming.DStream.checkpointData",
      "spark.streaming.DStream.graph",
      "spark.streaming.DStream.isInitialized",
      "spark.streaming.DStream.parentRememberDuration",
      "spark.streaming.DStream.initialize",
      "spark.streaming.DStream.validate",
      "spark.streaming.DStream.setContext",
      "spark.streaming.DStream.setGraph",
      "spark.streaming.DStream.remember",
      "spark.streaming.DStream.getOrCompute",
      "spark.streaming.DStream.generateJob",
      "spark.streaming.DStream.clearOldMetadata",
      "spark.streaming.DStream.addMetadata",
      "spark.streaming.DStream.updateCheckpointData",
      "spark.streaming.DStream.restoreCheckpointData",
      "spark.streaming.DStream.isTimeValid",
      "spark.streaming.StreamingContext.nextNetworkInputStreamId",
      "spark.streaming.StreamingContext.networkInputTracker",
      "spark.streaming.StreamingContext.checkpointDir",
      "spark.streaming.StreamingContext.checkpointDuration",
      "spark.streaming.StreamingContext.receiverJobThread",
      "spark.streaming.StreamingContext.scheduler",
      "spark.streaming.StreamingContext.initialCheckpoint",
      "spark.streaming.StreamingContext.getNewNetworkStreamId",
      "spark.streaming.StreamingContext.validate",
      "spark.streaming.StreamingContext.createNewSparkContext",
      "spark.streaming.StreamingContext.rddToFileName",
      "spark.streaming.StreamingContext.getSparkCheckpointDir",
      "spark.streaming.StreamingContext.env",
      "spark.streaming.StreamingContext.graph",
      "spark.streaming.StreamingContext.isCheckpointPresent"
    )
    val excludedPatterns = Seq(
      """^spark\.SparkContext\..*To.*Functions""",
      """^spark\.SparkContext\..*WritableConverter""",
      """^spark\.SparkContext\..*To.*Writable"""
    ).map(_.r)
    lazy val excludedByPattern =
      !excludedPatterns.map(_.findFirstIn(name)).filter(_.isDefined).isEmpty
    name.contains("$") || excludedNames.contains(name) || excludedByPattern
  }

  private def isExcludedByInterface(method: Method): Boolean = {
    val excludedInterfaces =
      Set("spark.Logging", "org.apache.hadoop.mapreduce.HadoopMapReduceUtil")
    def toComparisionKey(method: Method) =
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
      println(method)
    }
  }

  def main(args: Array[String]) {
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
    printMissingMethods(classOf[OrderedRDDFunctions[_, _]], classOf[JavaPairRDD[_, _]])
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
  }
}
