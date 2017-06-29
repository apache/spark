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

package org.apache.spark.ml.param.shared

import java.io.PrintWriter

import scala.reflect.ClassTag
import scala.xml.Utility

/**
 * Code generator for shared params (sharedParams.scala). Run under the Spark folder with
 * {{{
 *   build/sbt "mllib/runMain org.apache.spark.ml.param.shared.SharedParamsCodeGen"
 * }}}
 */
private[shared] object SharedParamsCodeGen {

  def main(args: Array[String]): Unit = {
    val params = Seq(
      ParamDesc[Double]("regParam", "regularization parameter (>= 0)",
        isValid = "ParamValidators.gtEq(0)"),
      ParamDesc[Int]("maxIter", "maximum number of iterations (>= 0)",
        isValid = "ParamValidators.gtEq(0)"),
      ParamDesc[String]("featuresCol", "features column name", Some("\"features\"")),
      ParamDesc[String]("labelCol", "label column name", Some("\"label\"")),
      ParamDesc[String]("predictionCol", "prediction column name", Some("\"prediction\"")),
      ParamDesc[String]("rawPredictionCol", "raw prediction (a.k.a. confidence) column name",
        Some("\"rawPrediction\"")),
      ParamDesc[String]("probabilityCol", "Column name for predicted class conditional" +
        " probabilities. Note: Not all models output well-calibrated probability estimates!" +
        " These probabilities should be treated as confidences, not precise probabilities",
        Some("\"probability\"")),
      ParamDesc[String]("varianceCol", "Column name for the biased sample variance of prediction"),
      ParamDesc[Double]("threshold",
        "threshold in binary classification prediction, in range [0, 1]", Some("0.5"),
        isValid = "ParamValidators.inRange(0, 1)", finalMethods = false),
      ParamDesc[Array[Double]]("thresholds", "Thresholds in multi-class classification" +
        " to adjust the probability of predicting each class." +
        " Array must have length equal to the number of classes, with values > 0" +
        " excepting that at most one value may be 0." +
        " The class with largest value p/t is predicted, where p is the original probability" +
        " of that class and t is the class's threshold",
        isValid = "(t: Array[Double]) => t.forall(_ >= 0) && t.count(_ == 0) <= 1",
        finalMethods = false),
      ParamDesc[String]("inputCol", "input column name"),
      ParamDesc[Array[String]]("inputCols", "input column names"),
      ParamDesc[String]("outputCol", "output column name", Some("uid + \"__output\"")),
      ParamDesc[Int]("checkpointInterval", "set checkpoint interval (>= 1) or " +
        "disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed " +
        "every 10 iterations", isValid = "(interval: Int) => interval == -1 || interval >= 1"),
      ParamDesc[Boolean]("fitIntercept", "whether to fit an intercept term", Some("true")),
      ParamDesc[String]("handleInvalid", "how to handle invalid entries. Options are skip (which " +
        "will filter out rows with bad values), or error (which will throw an error). More " +
        "options may be added later",
        isValid = "ParamValidators.inArray(Array(\"skip\", \"error\"))"),
      ParamDesc[Boolean]("standardization", "whether to standardize the training features" +
        " before fitting the model", Some("true")),
      ParamDesc[Long]("seed", "random seed", Some("this.getClass.getName.hashCode.toLong")),
      ParamDesc[Double]("elasticNetParam", "the ElasticNet mixing parameter, in range [0, 1]." +
        " For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty",
        isValid = "ParamValidators.inRange(0, 1)"),
      ParamDesc[Double]("tol", "the convergence tolerance for iterative algorithms (>= 0)",
        isValid = "ParamValidators.gtEq(0)"),
      ParamDesc[Double]("stepSize", "Step size to be used for each iteration of optimization (>" +
        " 0)", isValid = "ParamValidators.gt(0)"),
      ParamDesc[String]("weightCol", "weight column name. If this is not set or empty, we treat " +
        "all instance weights as 1.0"),
      ParamDesc[String]("solver", "the solver algorithm for optimization. If this is not set or " +
        "empty, default value is 'auto'", Some("\"auto\"")),
      ParamDesc[Int]("aggregationDepth", "suggested depth for treeAggregate (>= 2)", Some("2"),
        isValid = "ParamValidators.gtEq(2)", isExpertParam = true))

    val code = genSharedParams(params)
    val file = "src/main/scala/org/apache/spark/ml/param/shared/sharedParams.scala"
    val writer = new PrintWriter(file)
    writer.write(code)
    writer.close()
  }

  /** Description of a param. */
  private case class ParamDesc[T: ClassTag](
      name: String,
      doc: String,
      defaultValueStr: Option[String] = None,
      isValid: String = "",
      finalMethods: Boolean = true,
      isExpertParam: Boolean = false) {

    require(name.matches("[a-z][a-zA-Z0-9]*"), s"Param name $name is invalid.")
    require(doc.nonEmpty) // TODO: more rigorous on doc

    def paramTypeName: String = {
      val c = implicitly[ClassTag[T]].runtimeClass
      c match {
        case _ if c == classOf[Int] => "IntParam"
        case _ if c == classOf[Long] => "LongParam"
        case _ if c == classOf[Float] => "FloatParam"
        case _ if c == classOf[Double] => "DoubleParam"
        case _ if c == classOf[Boolean] => "BooleanParam"
        case _ if c.isArray && c.getComponentType == classOf[String] => s"StringArrayParam"
        case _ if c.isArray && c.getComponentType == classOf[Double] => s"DoubleArrayParam"
        case _ => s"Param[${getTypeString(c)}]"
      }
    }

    def valueTypeName: String = {
      val c = implicitly[ClassTag[T]].runtimeClass
      getTypeString(c)
    }

    private def getTypeString(c: Class[_]): String = {
      c match {
        case _ if c == classOf[Int] => "Int"
        case _ if c == classOf[Long] => "Long"
        case _ if c == classOf[Float] => "Float"
        case _ if c == classOf[Double] => "Double"
        case _ if c == classOf[Boolean] => "Boolean"
        case _ if c == classOf[String] => "String"
        case _ if c.isArray => s"Array[${getTypeString(c.getComponentType)}]"
      }
    }
  }

  /** Generates the HasParam trait code for the input param. */
  private def genHasParamTrait(param: ParamDesc[_]): String = {
    val name = param.name
    val Name = name(0).toUpper +: name.substring(1)
    val Param = param.paramTypeName
    val T = param.valueTypeName
    val doc = param.doc
    val defaultValue = param.defaultValueStr
    val defaultValueDoc = defaultValue.map { v =>
      s" (default: $v)"
    }.getOrElse("")
    val setDefault = defaultValue.map { v =>
      s"""
         |  setDefault($name, $v)
         |""".stripMargin
    }.getOrElse("")
    val isValid = if (param.isValid != "") {
      ", " + param.isValid
    } else {
      ""
    }
    val groupStr = if (param.isExpertParam) {
      Array("expertParam", "expertGetParam")
    } else {
      Array("param", "getParam")
    }
    val methodStr = if (param.finalMethods) {
      "final def"
    } else {
      "def"
    }

    val htmlCompliantDoc = Utility.escape(doc)

    s"""
      |/**
      | * Trait for shared param $name$defaultValueDoc.
      | */
      |private[ml] trait Has$Name extends Params {
      |
      |  /**
      |   * Param for $htmlCompliantDoc.
      |   * @group ${groupStr(0)}
      |   */
      |  final val $name: $Param = new $Param(this, "$name", "$doc"$isValid)
      |$setDefault
      |  /** @group ${groupStr(1)} */
      |  $methodStr get$Name: $T = $$($name)
      |}
      |""".stripMargin
  }

  /** Generates Scala source code for the input params with header. */
  private def genSharedParams(params: Seq[ParamDesc[_]]): String = {
    val header =
      """/*
        | * Licensed to the Apache Software Foundation (ASF) under one or more
        | * contributor license agreements.  See the NOTICE file distributed with
        | * this work for additional information regarding copyright ownership.
        | * The ASF licenses this file to You under the Apache License, Version 2.0
        | * (the "License"); you may not use this file except in compliance with
        | * the License.  You may obtain a copy of the License at
        | *
        | *    http://www.apache.org/licenses/LICENSE-2.0
        | *
        | * Unless required by applicable law or agreed to in writing, software
        | * distributed under the License is distributed on an "AS IS" BASIS,
        | * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        | * See the License for the specific language governing permissions and
        | * limitations under the License.
        | */
        |
        |package org.apache.spark.ml.param.shared
        |
        |import org.apache.spark.ml.param._
        |
        |// DO NOT MODIFY THIS FILE! It was generated by SharedParamsCodeGen.
        |
        |// scalastyle:off
        |""".stripMargin

    val footer = "// scalastyle:on\n"

    val traits = params.map(genHasParamTrait).mkString

    header + traits + footer
  }
}
