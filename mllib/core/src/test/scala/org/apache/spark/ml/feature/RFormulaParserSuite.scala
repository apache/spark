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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

class RFormulaParserSuite extends SparkFunSuite {
  private def checkParse(
      formula: String,
      label: String,
      terms: Seq[String],
      schema: StructType = new StructType): Unit = {
    val resolved = RFormulaParser.parse(formula).resolve(schema)
    assert(resolved.label == label)
    val simpleTerms = terms.map { t =>
      if (t.contains(":")) {
        t.split(":").toSeq
      } else {
        Seq(t)
      }
    }
    assert(resolved.terms == simpleTerms)
  }

  test("parse simple formulas") {
    checkParse("y ~ x", "y", Seq("x"))
    checkParse("y ~ x + x", "y", Seq("x"))
    checkParse("y~x+z", "y", Seq("x", "z"))
    checkParse("y ~   ._fo..o  ", "y", Seq("._fo..o"))
    checkParse("resp ~ A_VAR + B + c123", "resp", Seq("A_VAR", "B", "c123"))
  }

  test("parse dot") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
    checkParse("a ~ .", "a", Seq("b", "c"), schema)
  }

  test("parse deletion") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
    checkParse("a ~ c - b", "a", Seq("c"), schema)
  }

  test("parse additions and deletions in order") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
    checkParse("a ~ . - b + . - c", "a", Seq("b"), schema)
  }

  test("dot ignores complex column types") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "tinyint", false)
      .add("c", "map<string, string>", true)
    checkParse("a ~ .", "a", Seq("b"), schema)
  }

  test("parse intercept") {
    assert(RFormulaParser.parse("a ~ b").hasIntercept)
    assert(RFormulaParser.parse("a ~ b + 1").hasIntercept)
    assert(RFormulaParser.parse("a ~ b - 0").hasIntercept)
    assert(RFormulaParser.parse("a ~ b - 1 + 1").hasIntercept)
    assert(!RFormulaParser.parse("a ~ b + 0").hasIntercept)
    assert(!RFormulaParser.parse("a ~ b - 1").hasIntercept)
    assert(!RFormulaParser.parse("a ~ b + 1 - 1").hasIntercept)
  }

  test("parse interactions") {
    checkParse("y ~ a:b", "y", Seq("a:b"))
    checkParse("y ~ a:b + b:a", "y", Seq("a:b"))
    checkParse("y ~ ._a:._x", "y", Seq("._a:._x"))
    checkParse("y ~ foo:bar", "y", Seq("foo:bar"))
    checkParse("y ~ a : b : c", "y", Seq("a:b:c"))
    checkParse("y ~ q + a:b:c + b:c + c:d + z", "y", Seq("q", "a:b:c", "b:c", "c:d", "z"))
  }

  test("parse factor cross") {
    checkParse("y ~ a*b", "y", Seq("a", "b", "a:b"))
    checkParse("y ~ a*b + b*a", "y", Seq("a", "b", "a:b"))
    checkParse("y ~ ._a*._x", "y", Seq("._a", "._x", "._a:._x"))
    checkParse("y ~ foo*bar", "y", Seq("foo", "bar", "foo:bar"))
    checkParse("y ~ a * b * c", "y", Seq("a", "b", "a:b", "c", "a:c", "b:c", "a:b:c"))
  }

  test("interaction distributive") {
    checkParse("y ~ (a + b):c", "y", Seq("a:c", "b:c"))
    checkParse("y ~ c:(a + b)", "y", Seq("c:a", "c:b"))
  }

  test("factor cross distributive") {
    checkParse("y ~ (a + b)*c", "y", Seq("a", "b", "c", "a:c", "b:c"))
    checkParse("y ~ c*(a + b)", "y", Seq("c", "a", "b", "c:a", "c:b"))
  }

  test("parse power") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
      .add("d", "string", true)
    checkParse("a ~ (a + b)^2", "a", Seq("a", "b", "a:b"))
    checkParse("a ~ .^2", "a", Seq("b", "c", "d", "b:c", "b:d", "c:d"), schema)
    checkParse("a ~ .^3", "a", Seq("b", "c", "d", "b:c", "b:d", "c:d", "b:c:d"), schema)
    checkParse("a ~ .^3-.", "a", Seq("b:c", "b:d", "c:d", "b:c:d"), schema)
  }

  test("operator precedence") {
    checkParse("y ~ a*b:c", "y", Seq("a", "b:c", "a:b:c"))
    checkParse("y ~ (a*b):c", "y", Seq("a:c", "b:c", "a:b:c"))
  }

  test("parse basic interactions with dot") {
    val schema = (new StructType)
      .add("a", "int", true)
      .add("b", "long", false)
      .add("c", "string", true)
      .add("d", "string", true)
    checkParse("a ~ .:b", "a", Seq("b", "c:b", "d:b"), schema)
    checkParse("a ~ b:.", "a", Seq("b", "b:c", "b:d"), schema)
    checkParse("a ~ .:b:.:.:c:d:.", "a", Seq("b:c:d"), schema)
  }

  // Test data generated in R with terms.formula(y ~ .:., data = iris)
  test("parse all to all iris interactions") {
    val schema = (new StructType)
      .add("Sepal.Length", "double", true)
      .add("Sepal.Width", "double", true)
      .add("Petal.Length", "double", true)
      .add("Petal.Width", "double", true)
      .add("Species", "string", true)
    checkParse(
      "y ~ .:.",
      "y",
      Seq(
        "Sepal.Length",
        "Sepal.Width",
        "Petal.Length",
        "Petal.Width",
        "Species",
        "Sepal.Length:Sepal.Width",
        "Sepal.Length:Petal.Length",
        "Sepal.Length:Petal.Width",
        "Sepal.Length:Species",
        "Sepal.Width:Petal.Length",
        "Sepal.Width:Petal.Width",
        "Sepal.Width:Species",
        "Petal.Length:Petal.Width",
        "Petal.Length:Species",
        "Petal.Width:Species"),
      schema)
  }

  // Test data generated in R with terms.formula(y ~ .:. - Species:., data = iris)
  test("parse interaction negation with iris") {
    val schema = (new StructType)
      .add("Sepal.Length", "double", true)
      .add("Sepal.Width", "double", true)
      .add("Petal.Length", "double", true)
      .add("Petal.Width", "double", true)
      .add("Species", "string", true)
    checkParse("y ~ .:. - .:.", "y", Nil, schema)
    checkParse(
      "y ~ .:. - Species:.",
      "y",
      Seq(
        "Sepal.Length",
        "Sepal.Width",
        "Petal.Length",
        "Petal.Width",
        "Sepal.Length:Sepal.Width",
        "Sepal.Length:Petal.Length",
        "Sepal.Length:Petal.Width",
        "Sepal.Width:Petal.Length",
        "Sepal.Width:Petal.Width",
        "Petal.Length:Petal.Width"),
      schema)
  }
}
