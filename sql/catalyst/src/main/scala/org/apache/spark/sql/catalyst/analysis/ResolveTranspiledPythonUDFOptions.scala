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

import org.apache.spark.sql.catalyst.expressions.{TranspiledPythonUDF, TranspiledUDFParameter}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.TRANSPILED_PYTHON_UDF
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, DataType, DoubleType, IntegerType, LongType, ShortType, StringType}

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
 *
 * "integral", "integral32" and "fractional" narrow "numeric" rather than sitting beside it, and
 * that overlap is deliberate. Some Python operators only have an exact lowering once the operand's
 * *kind* of number is known -- `x ** 3` as `x*x*x` is exact on a bigint but rounds twice on a
 * double, and `a // b` needs `div`, which rejects fractional input -- so the transpiler tags those
 * options with the narrower category and leaves everything else on plain "numeric". A body using
 * no such operator produces the same options, and the same plan size, as before they existed.
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
            // The second half of the guard stops this firing every later iteration: categories
            // cleared and options resolved means there's nothing to do. It would still converge
            // without it, but it would re-walk every option each time round.
            case t: TranspiledPythonUDF if t.arguments.forall(_.resolved) &&
                (t.optionInputCategories.nonEmpty || !t.transpiledOptions.forall(_.resolved)) =>
              val args = t.arguments
              val pruned = if (t.optionInputCategories.isEmpty) {
                t
              } else {
                val argTypes = args.map(_.dataType)
                val kept = t.transpiledOptions.zip(t.optionInputCategories).collect {
                  case (option, categories) if optionMatchesTypes(categories, argTypes) => option
                }
                t.copy(transpiledOptions = kept, optionInputCategories = Nil)
              }
              // Type each `_udf_param_N` reference from the argument it stands for. The options are
              // unresolved until this runs, so the analyzer comes back after and coerces their
              // bodies like anything else.
              pruned.copy(transpiledOptions =
                pruned.transpiledOptions.map(TranspiledUDFParameter.resolveTypes(_, args)))
          }
      }
    }
  }

  // True when each declared category matches the corresponding argument type:
  // "numeric" -> Byte/Short/Integer/Long/DoubleType, "string" -> StringType,
  // "bool" -> BooleanType, "binary" -> BinaryType. "string" matches only StringType
  // (not BinaryType): a
  // bytes/BinaryType column is tagged "binary" instead, so the string lowerings
  // (e.g. `repeat`) never see it. Empty categories means "no restriction", so the
  // option is kept.
  //
  // The narrow numeric categories (see the class doc):
  // - "integral" -> Byte/Short/Integer/LongType, for `//`, the bitwise operators, the
  //   shifts, `**`, `round` and `min`/`max`, none of which have an exact double lowering.
  // - "integral32" -> Byte/Short/IntegerType, for true division: int32 values are all
  //   exactly representable as doubles, so the double division is correctly rounded and
  //   matches Python's int/int division. LongType doesn't (past 2^53 the cast to double
  //   rounds first), so bigint columns get no option and fall back.
  // - "fractional" -> DoubleType, which lets `a / b` lower for a double operand
  //   since Python also converts to double before dividing. Python's `float` is an
  //   IEEE binary64, so DoubleType is its counterpart; FloatType has none, which is
  //   the third exclusion below.
  //
  // Three deliberate exclusions keep the transpiled semantics faithful to Python:
  // - DecimalType is NOT "numeric" (nor "fractional", which it extends): Python
  //   receives decimal.Decimal objects, which raise TypeError when mixed with float
  //   literals and carry different precision semantics than Spark's decimal
  //   arithmetic, so decimal columns fall back to interpreted Python.
  // - FloatType is NOT "numeric" (nor "fractional"): Python has no single-precision
  //   float, so a FloatType value arrives in the UDF widened to a Python float (a
  //   double) and every operation on it is evaluated in double precision. Where an
  //   expression stays in FloatType -- which under ANSI coercion means both operands
  //   are floats, since an int or double literal promotes the whole expression to
  //   DoubleType -- Spark rounds to 24 bits per step where Python rounds to 53. So
  //   `x + y`, `x * y` and `x % y` on two float columns diverge; sampling random
  //   float pairs, `(x + y) * y` disagreed on 398 of 398 rows. It is not only a
  //   trailing-digit difference: float32 overflows to Infinity for operands Python
  //   handles comfortably (-9.726323523430302E29 * -260872823898112.0 is Infinity in
  //   FloatType and 2.5373334837038975E44 in double). Declaring a FloatType return
  //   type would hide the rounding for a single operation but not for a chain of
  //   them, and not the overflow at all -- and the return type isn't visible from
  //   here anyway. Float columns therefore fall back to interpreted Python.
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
        // Allow-lists rather than "NumericType minus the exclusions": the numeric
        // categories admit exactly the column types with a faithful Python
        // counterpart, and spelling that out means a numeric type added to Spark
        // later falls back until someone deliberately admits it, instead of being
        // silently swept in by an `isInstanceOf` and diverging.
        case ("numeric", ByteType | ShortType | IntegerType | LongType | DoubleType) => true
        case ("integral", ByteType | ShortType | IntegerType | LongType) => true
        case ("integral32", ByteType | ShortType | IntegerType) => true
        case ("fractional", DoubleType) => true
        case ("string", st: StringType) => st.isUTF8BinaryCollation
        case ("bool", dt) => dt.isInstanceOf[BooleanType]
        case ("binary", dt) => dt.isInstanceOf[BinaryType]
        case _ => false
      }
    }
  }
}
