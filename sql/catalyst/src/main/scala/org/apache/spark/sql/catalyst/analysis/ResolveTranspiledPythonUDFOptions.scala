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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.TranspiledPythonUDF
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPILED_PYTHON_UDF
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DecimalType, NumericType, StringType}

/**
 * Prunes the per-input-type options carried by a [[TranspiledPythonUDF]] down to those whose
 * declared categories match the resolved argument types.
 *
 * A Python operator such as `a + b` is overloaded for text, so the transpiler emits one option
 * per input-type variant -- a numeric `Add` and a string `concat`, say -- each tagged with the
 * input-type categories it expects. Those options are children of the node, so leaving a
 * type-incompatible one in place (a numeric `Add` over string columns) would make `CheckAnalysis`
 * reject the whole plan. We can only choose once the argument types are known, which is after
 * reference resolution -- hence a rule here rather than in the builder, which runs at
 * call-construction time before the columns are bound -- and we must run before `CheckAnalysis`.
 *
 * Matching is strict by category (a numeric option only for numeric columns, a string option only
 * for string columns). We deliberately do not lean on implicit type coercion, which would, e.g.,
 * make a numeric `Add` "valid" over a string column and silently diverge from Python's
 * `TypeError`. When no option matches, the list is emptied and `ConvertToCatalyst` falls back to
 * the original Python UDF.
 */
object ResolveTranspiledPythonUDFOptions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(TRANSPILED_PYTHON_UDF)) {
      plan
    } else {
      plan.resolveOperatorsWithPruning(_.containsPattern(TRANSPILED_PYTHON_UDF)) {
        case op if op.containsPattern(TRANSPILED_PYTHON_UDF) =>
          // Bottom-up so a nested TranspiledPythonUDF (a transpiled UDF feeding another) is pruned
          // -- and thus resolved -- before its parent's input types are inspected.
          op.transformExpressionsUpWithPruning(_.containsPattern(TRANSPILED_PYTHON_UDF)) {
            case t: TranspiledPythonUDF
                if t.optionInputCategories.nonEmpty && t.pythonUDFExpr.childrenResolved =>
              val argTypes = t.pythonUDFExpr.children.map(_.dataType)
              val kept = t.transpiledOptions.zip(t.optionInputCategories).collect {
                case (option, categories) if optionMatchesTypes(categories, argTypes) => option
              }
              t.copy(transpiledOptions = kept, optionInputCategories = Nil)
          }
      }
    }
  }

  // True when each declared category matches the corresponding argument type:
  // "numeric" -> NumericType, "string" -> StringType, "bool" -> BooleanType,
  // "binary" -> BinaryType. "string" matches only StringType (not BinaryType): a
  // bytes/BinaryType column is tagged "binary" instead, so the string lowerings
  // (e.g. `repeat`) never see it. Empty categories means "no restriction", so the
  // option is kept.
  //
  // Two deliberate exclusions keep the transpiled semantics faithful to Python:
  // - DecimalType is NOT "numeric": Python receives decimal.Decimal objects,
  //   which raise TypeError when mixed with float literals and carry different
  //   precision semantics than Spark's decimal arithmetic, so decimal columns
  //   fall back to interpreted Python.
  // - "string" requires the default UTF8_BINARY collation: under a non-binary
  //   collation (e.g. UTF8_LCASE) Spark's `=`/`<`/`concat` follow collation
  //   rules while Python compares codepoints, so `'abc' == 'ABC'` would return
  //   true where Python returns False.
  private def optionMatchesTypes(categories: Seq[String], argTypes: Seq[DataType]): Boolean = {
    if (categories.isEmpty) {
      true
    } else if (categories.length != argTypes.length) {
      false
    } else {
      categories.zip(argTypes).forall {
        case ("numeric", dt) => dt.isInstanceOf[NumericType] && !dt.isInstanceOf[DecimalType]
        case ("string", st: StringType) => st.isUTF8BinaryCollation
        case ("bool", dt) => dt.isInstanceOf[BooleanType]
        case ("binary", dt) => dt.isInstanceOf[BinaryType]
        case _ => false
      }
    }
  }
}
