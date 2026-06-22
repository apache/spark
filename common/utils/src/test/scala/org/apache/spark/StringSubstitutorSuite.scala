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
package org.apache.spark

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

class StringSubstitutorSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  test("basic substitution") {
    val sub = new StringSubstitutor(Map("a" -> "X", "b" -> "Y"))
    assert(sub.replace("${a}") === "X")
    assert(sub.replace("pre${a}post") === "preXpost")
    assert(sub.replace("${a}${b}") === "XY")
    assert(sub.replace("${a}-${b}") === "X-Y")
  }

  test("no placeholders, empty and null input") {
    val sub = new StringSubstitutor(Map("a" -> "X"))
    assert(sub.replace("hello") === "hello")
    assert(sub.replace("") === "")
    assert(sub.replace(null) === null)
  }

  test("non-string values are rendered with toString") {
    val sub = new StringSubstitutor(Map("n" -> 42, "b" -> true))
    assert(sub.replace("${n}/${b}") === "42/true")
  }

  test("undefined variable throws by default") {
    val sub = new StringSubstitutor(Map.empty[String, Any])
    val e = intercept[IllegalArgumentException](sub.replace("${x}"))
    assert(e.getMessage.contains("x"))
  }

  test("undefined variable is left as-is when exceptions are disabled") {
    val sub =
      new StringSubstitutor(Map.empty[String, Any], enableUndefinedVariableException = false)
    assert(sub.replace("a${x}b") === "a${x}b")
  }

  test("default value is used only when the variable is undefined") {
    assert(new StringSubstitutor(Map.empty[String, Any]).replace("${x:-def}") === "def")
    assert(new StringSubstitutor(Map("x" -> "X")).replace("${x:-def}") === "X")
    // Empty variable name: the delimiter is at index 0, so the text after it is the default.
    assert(new StringSubstitutor(Map.empty[String, Any]).replace("${:-fallback}") === "fallback")
  }

  test("a default-value substitution is followed by a further substitution") {
    // After a default-value replacement the buffer shifts, so the scan length must be adjusted
    // for the later ${b} to still be expanded.
    val sub = new StringSubstitutor(Map("b" -> "Y"))
    assert(sub.replace("${a:-LONG_DEFAULT}${b}") === "LONG_DEFAULTY")
  }

  test("a resolved value longer than its placeholder is followed by a further substitution") {
    // The replacement is longer than its placeholder (a growing buffer); the later ${b} must
    // still be expanded.
    val sub = new StringSubstitutor(Map("a" -> "LONG_VALUE", "b" -> "Y"))
    assert(sub.replace("${a}${b}") === "LONG_VALUEY")
  }

  test("escaped placeholder is not substituted") {
    // preserveEscapes = true (default): the escape is kept and the placeholder is not expanded,
    // but the scan resumes, so a later resolvable placeholder is still expanded.
    assert(new StringSubstitutor(Map("a" -> "X")).replace("$${a}") === "$${a}")
    assert(new StringSubstitutor(Map("b" -> "Y")).replace("$${a}${b}") === "$${a}Y")
    // preserveEscapes = false: the escape is removed and the placeholder is still not expanded;
    // again the scan resumes for a later placeholder.
    assert(new StringSubstitutor(Map("a" -> "X"), preserveEscapes = false).replace("$${a}")
      === "${a}")
    assert(new StringSubstitutor(Map("b" -> "Y"), preserveEscapes = false).replace("$${a}${b}")
      === "${a}Y")
  }

  test("nested references are expanded only when enabled") {
    val resolver = Map[String, Any]("a" -> "${b}", "b" -> "Y")
    assert(new StringSubstitutor(resolver).replace("${a}") === "${b}")
    assert(new StringSubstitutor(resolver, enableSubstitutionInVariables = true).replace("${a}")
      === "Y")
  }

  test("cyclic references are detected when nested expansion is enabled") {
    val sub = new StringSubstitutor(
      Map("a" -> "${b}", "b" -> "${a}"), enableSubstitutionInVariables = true)
    val e = intercept[IllegalStateException](sub.replace("${a}"))
    assert(e.getMessage.startsWith("Infinite loop in property interpolation of"))
    assert(e.getMessage.contains("a") || e.getMessage.contains("b"))
  }

  test("an unclosed placeholder is left as-is") {
    val sub = new StringSubstitutor(Map("a" -> "X"))
    assert(sub.replace("text ${unclosed") === "text ${unclosed")
    // A resolved placeholder earlier in the string does not stop the scan from reaching and
    // leaving the unclosed tail.
    assert(sub.replace("${a} and ${unclosed") === "X and ${unclosed")
  }

  test("an empty default value collapses the placeholder to the empty string") {
    val sub = new StringSubstitutor(Map.empty[String, Any])
    assert(sub.replace("prefix${x:-}suffix") === "prefixsuffix")
    // A later placeholder is still expanded after the empty-default replacement.
    assert(sub.replace("${x:-}${y:-Z}") === "Z")
  }

  test("custom prefix, suffix, escape, and value delimiter") {
    val sub = new StringSubstitutor(
      Map("x" -> "X"), prefix = "%{", suffix = "}", escape = '%', valueDelimiter = "|")
    assert(sub.replace("%{x}") === "X")
    assert(sub.replace("pre%{x}post") === "preXpost")
    // The custom value delimiter supplies the default for an undefined variable.
    assert(sub.replace("%{y|def}") === "def")
    // The custom escape char guards the custom prefix (preserveEscapes defaults to true).
    assert(sub.replace("%%{x}") === "%%{x}")
  }

  test("scan continues past an unresolved placeholder to a later resolved one") {
    // With exceptions disabled, an unresolved placeholder is left untouched but does not stop the
    // scan from reaching and expanding a later resolvable placeholder.
    val sub = new StringSubstitutor(Map("x" -> "X"), enableUndefinedVariableException = false)
    assert(sub.replace("${undefined} ${x}") === "${undefined} X")
  }

  test("a null resolver value renders as the string \"null\"") {
    // The sole caller (ErrorClassesJSONReader) sanitizes a null parameter to "null", so a null
    // value is rendered the same way here rather than NPEing on toString or being treated as
    // undefined.
    val resolver = Map[String, Any]("x" -> null)
    assert(new StringSubstitutor(resolver).replace("${x}") === "null")
    assert(new StringSubstitutor(resolver).replace("a${x}b") === "anullb")
    // The variable is resolved (to "null"), so its default value is not used.
    assert(new StringSubstitutor(resolver).replace("${x:-def}") === "null")
  }

  test("placeholders inside a default value are not expanded") {
    // The first '}' closes the placeholder, so the default value is the literal text up to it and
    // the inner ${x} is not treated as a nested reference.
    val sub = new StringSubstitutor(Map("x" -> "X"))
    assert(sub.replace("${undefined:-${x}}") === "${x}")
  }

  test("an empty placeholder with no default is undefined") {
    val e = intercept[IllegalArgumentException](
      new StringSubstitutor(Map.empty[String, Any]).replace("${}"))
    assert(e.getMessage.contains("Undefined variable"))
  }
}
