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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{ArrayTransform, AttributeReference, BooleanLiteral, Cast, CheckOverflowInTableInsert, CreateNamedStruct, EvalMode, GetStructField, IntegerLiteral, LambdaFunction, LongLiteral, MapFromArrays, StringLiteral}
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull, StaticInvoke}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, InsertAction, MergeAction, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.IntegerType

class AlignMergeAssignmentsSuite extends AlignAssignmentsSuiteBase {

  test("align assignments (primitive types)") {
    val (matchedActions, notMatchedActions, notMatchedBySourceActions) =
      parseAndAlignAssignments(
        """MERGE INTO primitive_table t USING primitive_table_src s
          |ON t.l = s.l
          |WHEN MATCHED THEN
          | UPDATE SET t.txt = 'a', t.i = s.i
          |WHEN NOT MATCHED THEN
          | INSERT *
          |WHEN NOT MATCHED BY SOURCE THEN
          | UPDATE SET t.txt = "error", t.i = CAST(null AS INT)""".stripMargin)

    matchedActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, AssertNotNull(iValue: AttributeReference, _)),
              Assignment(l: AttributeReference, lValue: AttributeReference),
              Assignment(txt: AttributeReference, StringLiteral("a"))) =>

            assert(i.name == "i" && iValue.name == "i" && i != iValue)
            assert(l.name == "l" && l == lValue)
            assert(txt.name == "txt")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedActions match {
      case Seq(InsertAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, AssertNotNull(iValue: AttributeReference, _)),
              Assignment(l: AttributeReference, lValue: AttributeReference),
              Assignment(txt: AttributeReference, txtValue: AttributeReference)) =>

            assert(i.name == "i" && iValue.name == "i" && i != iValue)
            assert(l.name == "l" && lValue.name == "l" && l != lValue)
            assert(txt.name == "txt" && txtValue.name == "txt" && txt != txtValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedBySourceActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, AssertNotNull(_: Cast, _)),
              Assignment(l: AttributeReference, lValue: AttributeReference),
              Assignment(txt: AttributeReference, StringLiteral("error"))) =>

            assert(i.name == "i")
            assert(l.name == "l" && l == lValue)
            assert(txt.name == "txt")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
  }

  test("align assignments (top-level structs)") {
    val (matchedActions, notMatchedActions, notMatchedBySourceActions) =
      parseAndAlignAssignments(
        """MERGE INTO nested_struct_table t USING nested_struct_table_src s
          |ON t.i = s.i
          |WHEN MATCHED THEN
          | UPDATE SET t.s = named_struct('n_s', named_struct('dn_i', 1, 'dn_l', 100L), 'n_i', 1)
          |WHEN NOT MATCHED THEN
          | INSERT (txt, s, i) VALUES (
          |  'new',
          |   named_struct('n_s', named_struct('dn_i', 1, 'dn_l', 100L), 'n_i', 1),
          |   1)
          |WHEN NOT MATCHED BY SOURCE THEN
          | UPDATE SET t.s = named_struct('n_s', named_struct('dn_i', 1, 'dn_l', 100L), 'n_i', 1)
          |""".stripMargin)

    def checkStruct(value: CreateNamedStruct): Unit = {
      value.children match {
        case Seq(
            StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
            StringLiteral("n_s"), nsValue: CreateNamedStruct) =>

          nsValue.children match {
            case Seq(
                StringLiteral("dn_i"), GetStructField(_, _, Some("dn_i")),
                StringLiteral("dn_l"), GetStructField(_, _, Some("dn_l"))) =>
              // OK

            case nsValueChildren =>
              fail(s"Unexpected children for 's.n_s': $nsValueChildren")
          }

        case sValueChildren =>
          fail(s"Unexpected children for 's': $sValueChildren")
      }
    }

    matchedActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, iValue: AttributeReference),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, txtValue: AttributeReference)) =>

            assert(i.name == "i" && i == iValue)

            assert(s.name == "s")
            checkStruct(sValue)

            assert(txt.name == "txt" && txt == txtValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedActions match {
      case Seq(InsertAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, IntegerLiteral(1)),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, StringLiteral("new"))) =>

            assert(i.name == "i")

            assert(s.name == "s")
            checkStruct(sValue)

            assert(txt.name == "txt")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedBySourceActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, iValue: AttributeReference),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, txtValue: AttributeReference)) =>

            assert(i.name == "i" && i == iValue)

            assert(s.name == "s")
            checkStruct(sValue)

            assert(txt.name == "txt" && txt == txtValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
  }

  test("align UPDATE assignments with references to nested attributes on both sides") {
    val (matchedActions, _, _) =
      parseAndAlignAssignments(
        """MERGE INTO nested_struct_table_src t USING nested_struct_table_src src
          |ON t.i = src.i
          |WHEN MATCHED THEN
          | UPDATE SET t.s.n_s = src.s.n_s
          |""".stripMargin)

    matchedActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, iValue: AttributeReference),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, txtValue: AttributeReference)) =>

            assert(i.name == "i" && i == iValue)

            assert(s.name == "s")
            sValue.children match {
              case Seq(
                  StringLiteral("n_i"),
                  GetStructField(_, _, Some("n_i")),
                  StringLiteral("n_s"),
                  GetStructField(source: AttributeReference, _, Some("n_s"))) =>

                assert(source.name == "s" && s != source)

              case sValueChildren =>
                fail(s"Unexpected children for 's': $sValueChildren")
            }

            assert(txt.name == "txt" && txt == txtValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
  }

  test("align assignments (nested structs)") {
    val (matchedActions, notMatchedActions, notMatchedBySourceActions) =
      parseAndAlignAssignments(
        """MERGE INTO nested_struct_table t USING nested_struct_table_src s
          |ON t.i = s.i
          |WHEN MATCHED THEN
          | UPDATE SET t.s.n_s = named_struct('dn_l', 1L, 'dn_i', 1)
          |WHEN NOT MATCHED THEN
          | INSERT (txt, s, i) VALUES (
          |  'new',
          |   named_struct('n_i', 1, 'n_s', named_struct('dn_l', 1L, 'dn_i', 1)),
          |   1)
          |WHEN NOT MATCHED BY SOURCE THEN
          | UPDATE SET t.s.n_s = named_struct('dn_l', 1L, 'dn_i', 1)
          |""".stripMargin)

    def checkNestedStruct(value: CreateNamedStruct): Unit = {
      value.children match {
        case Seq(
            StringLiteral("dn_i"), GetStructField(_, _, Some("dn_i")),
            StringLiteral("dn_l"), GetStructField(_, _, Some("dn_l"))) =>
          // OK

        case nsValueChildren =>
          fail(s"Unexpected children for 's.n_s': $nsValueChildren")
      }
    }

    matchedActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, iValue: AttributeReference),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, txtValue: AttributeReference)) =>

            assert(i.name == "i" && i == iValue)

            assert(s.name == "s")
            sValue.children match {
              case Seq(
                  StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
                  StringLiteral("n_s"), nsValue: CreateNamedStruct) =>
                checkNestedStruct(nsValue)

              case sValueChildren =>
                fail(s"Unexpected children for 's': $sValueChildren")
            }

            assert(txt.name == "txt" && txt == txtValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedActions match {
      case Seq(InsertAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, IntegerLiteral(1)),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, StringLiteral("new"))) =>

            assert(i.name == "i")

            assert(s.name == "s")
            sValue.children match {
              case Seq(
                  StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
                  StringLiteral("n_s"), nsValue: CreateNamedStruct) =>
                checkNestedStruct(nsValue)

              case sValueChildren =>
                fail(s"Unexpected children for 's': $sValueChildren")
            }

            assert(txt.name == "txt")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedBySourceActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(i: AttributeReference, iValue: AttributeReference),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(txt: AttributeReference, txtValue: AttributeReference)) =>

            assert(i.name == "i" && i == iValue)

            assert(s.name == "s")
            sValue.children match {
              case Seq(
                  StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
                  StringLiteral("n_s"), nsValue: CreateNamedStruct) =>
                checkNestedStruct(nsValue)

              case sValueChildren =>
                fail(s"Unexpected children for 's': $sValueChildren")
            }

            assert(txt.name == "txt" && txt == txtValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
  }

  test("align assignments (char and varchar types)") {
    val (matchedActions, notMatchedActions, notMatchedBySourceActions) =
      parseAndAlignAssignments(
        """MERGE INTO char_varchar_table t USING char_varchar_table src
          |ON t.c = src.c
          |WHEN MATCHED THEN
          | UPDATE SET
          |  c = 'a',
          |  a = array(named_struct('n_i', 1, 'n_vc', 3)),
          |  s.n_vc = 'a',
          |  mv = map('v', named_struct('n_vc', 'a', 'n_i', 1)),
          |  mk = map(named_struct('n_vc', 'a', 'n_i', 1), 'v')
          |WHEN NOT MATCHED THEN
          | INSERT (c, a, s, mv, mk) VALUES (
          |  'a',
          |  array(named_struct('n_i', 1, 'n_vc', 3)),
          |  named_struct('n_vc', 3, 'n_i', 1),
          |  map('v', named_struct('n_vc', 'a', 'n_i', 1)),
          |  map(named_struct('n_vc', 'a', 'n_i', 1), 'v'))
          |WHEN NOT MATCHED BY SOURCE THEN
          | UPDATE SET
          |  c = 'a',
          |  a = array(named_struct('n_i', 1, 'n_vc', 3)),
          |  s = named_struct('n_vc', 3, 'n_i', 1),
          |  mv = map('v', named_struct('n_vc', 'a', 'n_i', 1)),
          |  mk = map(named_struct('n_vc', 'a', 'n_i', 1), 'v')
          |""".stripMargin)

    def checkStruct(value: CreateNamedStruct): Unit = {
      value.children match {
        case Seq(
            StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
            StringLiteral("n_vc"), invoke: StaticInvoke) =>

          assert(invoke.arguments.length == 2)
          assert(invoke.functionName == "varcharTypeWriteSideCheck")

        case sValueChildren =>
          fail(s"Unexpected children for 's': $sValueChildren")
      }
    }

    def checkArray(value: ArrayTransform): Unit = {
      val lambda = value.function.asInstanceOf[LambdaFunction]
      lambda.function match {
        case CreateNamedStruct(Seq(
            StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
            StringLiteral("n_vc"), invoke: StaticInvoke)) =>

          assert(invoke.arguments.length == 2)
          assert(invoke.functionName == "varcharTypeWriteSideCheck")

        case func =>
          fail(s"Unexpected lambda function: $func")
      }
    }

    def checkMapWithStructKey(value: MapFromArrays): Unit = {
      val keyTransform = value.left.asInstanceOf[ArrayTransform]
      val keyLambda = keyTransform.function.asInstanceOf[LambdaFunction]
      keyLambda.function match {
        case CreateNamedStruct(Seq(
            StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
            StringLiteral("n_vc"), invoke: StaticInvoke)) =>

          assert(invoke.arguments.length == 2)
          assert(invoke.functionName == "varcharTypeWriteSideCheck")

        case func =>
          fail(s"Unexpected key lambda function: $func")
      }
    }

    def checkMapWithStructValue(value: MapFromArrays): Unit = {
      val valueTransform = value.right.asInstanceOf[ArrayTransform]
      val valueLambda = valueTransform.function.asInstanceOf[LambdaFunction]
      valueLambda.function match {
        case CreateNamedStruct(Seq(
            StringLiteral("n_i"), GetStructField(_, _, Some("n_i")),
            StringLiteral("n_vc"), invoke: StaticInvoke)) =>

          assert(invoke.arguments.length == 2)
          assert(invoke.functionName == "varcharTypeWriteSideCheck")

        case func =>
          fail(s"Unexpected key lambda function: $func")
      }
    }

    matchedActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(c: AttributeReference, cValue: StaticInvoke),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(a: AttributeReference, aValue: ArrayTransform),
              Assignment(mk: AttributeReference, mkValue: MapFromArrays),
              Assignment(mv: AttributeReference, mvValue: MapFromArrays)) =>

            assert(c.name == "c")
            assert(cValue.arguments.length == 2)
            assert(cValue.functionName == "charTypeWriteSideCheck")

            assert(s.name == "s")
            checkStruct(sValue)

            assert(a.name == "a")
            checkArray(aValue)

            assert(mk.name == "mk")
            checkMapWithStructKey(mkValue)

            assert(mv.name == "mv")
            checkMapWithStructValue(mvValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedActions match {
      case Seq(InsertAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(c: AttributeReference, cValue: StaticInvoke),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(a: AttributeReference, aValue: ArrayTransform),
              Assignment(mk: AttributeReference, mkValue: MapFromArrays),
              Assignment(mv: AttributeReference, mvValue: MapFromArrays)) =>

            assert(c.name == "c")
            assert(cValue.arguments.length == 2)
            assert(cValue.functionName == "charTypeWriteSideCheck")

            assert(s.name == "s")
            checkStruct(sValue)

            assert(a.name == "a")
            checkArray(aValue)

            assert(mk.name == "mk")
            checkMapWithStructKey(mkValue)

            assert(mv.name == "mv")
            checkMapWithStructValue(mvValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedBySourceActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(c: AttributeReference, cValue: StaticInvoke),
              Assignment(s: AttributeReference, sValue: CreateNamedStruct),
              Assignment(a: AttributeReference, aValue: ArrayTransform),
              Assignment(mk: AttributeReference, mkValue: MapFromArrays),
              Assignment(mv: AttributeReference, mvValue: MapFromArrays)) =>

            assert(c.name == "c")
            assert(cValue.arguments.length == 2)
            assert(cValue.functionName == "charTypeWriteSideCheck")

            assert(s.name == "s")
            checkStruct(sValue)

            assert(a.name == "a")
            checkArray(aValue)

            assert(mk.name == "mk")
            checkMapWithStructKey(mkValue)

            assert(mv.name == "mv")
            checkMapWithStructValue(mvValue)

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
  }

  test("conflicting UPDATE assignments") {
    Seq(StoreAssignmentPolicy.ANSI, StoreAssignmentPolicy.STRICT).foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy.toString) {
        Seq("WHEN MATCHED", "WHEN NOT MATCHED BY SOURCE").foreach { clause =>
          // two updates to a top-level column
          assertAnalysisException(
            s"""MERGE INTO primitive_table t USING primitive_table_src s
               |ON t.l = s.l
               |$clause THEN
               | UPDATE SET t.txt = 'a', t.txt = 'b'
               |""".stripMargin,
            "Multiple assignments for 'txt': 'a', 'b'")

          // two updates to a nested column
          val e = intercept[AnalysisException] {
            parseAndResolve(
              s"""MERGE INTO nested_struct_table t USING nested_struct_table src
                 |ON t.i = src.i
                 |$clause THEN
                 | UPDATE SET s.n_i = 1, s.n_s = null, s.n_i = -1
                 |""".stripMargin
            )
          }
          if (policy == StoreAssignmentPolicy.ANSI) {
            checkError(
              exception = e,
              condition = "DATATYPE_MISMATCH.INVALID_ROW_LEVEL_OPERATION_ASSIGNMENTS",
              parameters = Map(
                "sqlExpr" -> "\"s.n_i = 1\", \"s.n_s = NULL\", \"s.n_i = -1\"",
                "errors" -> "\n- Multiple assignments for 's.n_i': 1, -1")
            )
          } else {
            checkError(
              exception = e,
              condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST",
              parameters = Map(
                "tableName" -> "``",
                "colName" -> "`s`.`n_s`",
                "srcType" -> "\"VOID\"",
                "targetType" -> "\"STRUCT<DN_I:INT,DN_L:BIGINT>\"")
            )
          }

          // conflicting updates to a nested struct and its fields
          assertAnalysisException(
            s"""MERGE INTO nested_struct_table t USING nested_struct_table src
               |ON t.i = src.i
               |$clause THEN
               | UPDATE SET s.n_s.dn_i = 1, s.n_s = named_struct('dn_i', 1, 'dn_l', 1L)
               |""".stripMargin,
            "Conflicting assignments for 's.n_s'",
            "t.s.`n_s` = named_struct('dn_i', 1, 'dn_l', 1L)",
            "t.s.`n_s`.`dn_i` = 1")
        }
      }
    }
  }

  test("invalid INSERT assignments") {
    assertAnalysisException(
      """MERGE INTO primitive_table t USING primitive_table src
        |ON t.i = src.i
        |WHEN NOT MATCHED THEN
        | INSERT (i, l, txt, txt) VALUES (src.i, src.l, src.txt, src.txt)
        |""".stripMargin,
      "Multiple assignments for 'txt'")

    assertAnalysisException(
      """MERGE INTO nested_struct_table t USING nested_struct_table src
        |ON t.i = src.i
        |WHEN NOT MATCHED THEN
        | INSERT (s.n_i) VALUES (1)
        |""".stripMargin,
      "INSERT assignment keys cannot be nested fields: t.s.`n_i` = 1")
  }

  test("updates to nested structs in arrays") {
    Seq(StoreAssignmentPolicy.ANSI, StoreAssignmentPolicy.STRICT).foreach { policy =>
      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> policy.toString) {
        assertAnalysisException(
          """MERGE INTO map_array_table t USING map_array_table s
            |ON t.i = s.i
            |WHEN MATCHED THEN
            | UPDATE SET t.a.i1 = 1
            |""".stripMargin,
          "Updating nested fields is only supported for StructType but 'a' is of type ArrayType")
      }
    }
  }

  test("ANSI mode in UPDATE assignments") {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString) {
      Seq("WHEN MATCHED", "WHEN NOT MATCHED BY SOURCE").foreach { clause =>
        val plan1 = parseAndResolve(
          s"""MERGE INTO primitive_table t USING primitive_table_src src
             |ON t.l = src.l
             |$clause THEN
             | UPDATE SET i = NULL
             |""".stripMargin)
        assertNullCheckExists(plan1, Seq("i"))

        val plan2 = parseAndResolve(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_i = NULL
             |""".stripMargin)
        assertNullCheckExists(plan2, Seq("s", "n_i"))

        val plan3 = parseAndResolve(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_s.dn_i = NULL
             |""".stripMargin)
        assertNullCheckExists(plan3, Seq("s", "n_s", "dn_i"))

        val plan4 = parseAndResolve(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_s = named_struct('dn_i', NULL, 'dn_l', 1L)
             |""".stripMargin)
        assertNullCheckExists(plan4, Seq("s", "n_s", "dn_i"))

        val e = intercept[AnalysisException] {
          parseAndResolve(
            s"""MERGE INTO nested_struct_table t USING nested_struct_table src
               |ON t.i = src.i
               |$clause THEN
               | UPDATE SET s.n_s = named_struct('dn_i', 1)
               |""".stripMargin
          )
        }
        checkError(
          exception = e,
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          parameters = Map("tableName" -> "``", "colName" -> "`s`.`n_s`.`dn_l`")
        )

        // ANSI mode does NOT allow string to int casts
        assertAnalysisException(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_s = named_struct('dn_i', 'string-value', 'dn_l', 1L)
             |""".stripMargin,
          "Cannot safely cast")

        val (matchedActions, _, notMatchedBySourceActions) =
          parseAndAlignAssignments(
            s"""MERGE INTO primitive_table t USING primitive_table_src src
               |ON t.i = src.i
               |$clause THEN
               | UPDATE SET i = 1L, txt = 'new', l = 10L
               |""".stripMargin)

        val actions = if (matchedActions.nonEmpty) matchedActions else notMatchedBySourceActions
        actions match {
          case Seq(UpdateAction(_, assignments)) =>
            assignments match {
              case Seq(
                  Assignment(
                      i: AttributeReference,
                      CheckOverflowInTableInsert(
                          Cast(LongLiteral(1L), IntegerType, _, EvalMode.ANSI), _)),
                  Assignment(l: AttributeReference, LongLiteral(10L)),
                  Assignment(txt: AttributeReference, StringLiteral("new"))) =>

                assert(i.name == "i")
                assert(l.name == "l")
                assert(txt.name == "txt")

              case assignments =>
                fail(s"Unexpected assignments: $assignments")
            }

          case other =>
            fail(s"Unexpected actions: $other")
        }
      }
    }
  }

  test("ANSI mode in INSERT assignments") {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.ANSI.toString) {
      val plan1 = parseAndResolve(
        """MERGE INTO primitive_table t USING primitive_table_src src
          |ON t.l = src.l
          |WHEN NOT MATCHED THEN
          | INSERT (i, l, txt) VALUES (NULL, 1, 'value')
          |""".stripMargin)
      assertNullCheckExists(plan1, Seq("i"))

      // ANSI mode does NOT allow string to int casts
      assertAnalysisException(
        """MERGE INTO primitive_table t USING primitive_table_src src
          |ON t.l = src.l
          |WHEN NOT MATCHED THEN
          | INSERT (i, l, txt) VALUES ('1', 1, 'value')
          |""".stripMargin,
        "Cannot safely cast")

      val (_, notMatchedActions, _) =
        parseAndAlignAssignments(
          """MERGE INTO primitive_table t USING primitive_table_src src
            |ON t.i = src.i
            |WHEN NOT MATCHED THEN
            | INSERT (i, l, txt) VALUES (1L, 10L, 'new')
            |""".stripMargin)

      notMatchedActions match {
        case Seq(InsertAction(_, assignments)) =>
          assignments match {
            case Seq(
                Assignment(
                    i: AttributeReference,
                    CheckOverflowInTableInsert(
                        Cast(LongLiteral(1L), IntegerType, _, EvalMode.ANSI), _)),
                Assignment(l: AttributeReference, LongLiteral(10L)),
                Assignment(txt: AttributeReference, StringLiteral("new"))) =>

              assert(i.name == "i")
              assert(l.name == "l")
              assert(txt.name == "txt")

            case assignments =>
              fail(s"Unexpected assignments: $assignments")
          }

        case other =>
          fail(s"Unexpected actions: $other")
      }
    }
  }

  test("strict mode in UPDATE assignments") {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.STRICT.toString) {
      Seq("WHEN MATCHED", "WHEN NOT MATCHED BY SOURCE").foreach { clause =>
        val plan1 = parseAndResolve(
          s"""MERGE INTO primitive_table t USING primitive_table_src src
             |ON t.l = src.l
             |$clause THEN
             | UPDATE SET i = CAST(NULL AS INT)
             |""".stripMargin)
        assertNullCheckExists(plan1, Seq("i"))

        val plan2 = parseAndResolve(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_i = CAST(NULL AS INT)
             |""".stripMargin)
        assertNullCheckExists(plan2, Seq("s", "n_i"))

        val plan3 = parseAndResolve(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_s.dn_i = CAST(NULL AS INT)
             |""".stripMargin)
        assertNullCheckExists(plan3, Seq("s", "n_s", "dn_i"))

        val plan4 = parseAndResolve(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_s = named_struct('dn_i', CAST (NULL AS INT), 'dn_l', 1L)
             |""".stripMargin)
        assertNullCheckExists(plan4, Seq("s", "n_s", "dn_i"))

        val e = intercept[AnalysisException] {
          parseAndResolve(
            s"""MERGE INTO nested_struct_table t USING nested_struct_table src
               |ON t.i = src.i
               |$clause THEN
               | UPDATE SET s.n_s = named_struct('dn_i', 1)
               |""".stripMargin
          )
        }
        checkError(
          exception = e,
          condition = "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA",
          parameters = Map("tableName" -> "``", "colName" -> "`s`.`n_s`.`dn_l`")
        )

        // strict mode does NOT allow string to int casts
        assertAnalysisException(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET s.n_s = named_struct('dn_i', 'string-value', 'dn_l', 1L)
             |""".stripMargin,
          "Cannot safely cast")

        // strict mode does not allow long to int casts
        assertAnalysisException(
          s"""MERGE INTO nested_struct_table t USING nested_struct_table src
             |ON t.i = src.i
             |$clause THEN
             | UPDATE SET i = 1L
             |""".stripMargin,
          "Cannot safely cast")
      }
    }
  }

  test("legacy mode assignments") {
    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> StoreAssignmentPolicy.LEGACY.toString) {
      assertAnalysisException(
        s"""MERGE INTO nested_struct_table t USING nested_struct_table src
           |ON t.i = src.i
           |WHEN MATCHED THEN
           | UPDATE SET i = 1L
           |""".stripMargin,
        "LEGACY store assignment policy is disallowed in Spark data source V2")
    }
  }

  test("align assignments with default values") {
    val (matchedActions, notMatchedActions, notMatchedBySourceActions) =
      parseAndAlignAssignments(
        """MERGE INTO default_values_table t USING default_values_table s
          |ON t.b = s.b
          |WHEN MATCHED THEN
          | UPDATE SET t.i = DEFAULT
          |WHEN NOT MATCHED AND (s.i = 1) THEN
          | INSERT (b) VALUES (false)
          |WHEN NOT MATCHED THEN
          | INSERT (i, b) VALUES (DEFAULT, false)
          |WHEN NOT MATCHED BY SOURCE THEN
          | UPDATE SET t.i = DEFAULT""".stripMargin)

    matchedActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(b: AttributeReference, bValue: AttributeReference),
              Assignment(i: AttributeReference, IntegerLiteral(42))) =>

            assert(b.name == "b" && b == bValue)
            assert(i.name == "i")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    assert(notMatchedActions.length == 2)
    notMatchedActions(0) match {
      case InsertAction(Some(_), assignments) =>
        assignments match {
          case Seq(
              Assignment(b: AttributeReference, BooleanLiteral(false)),
              Assignment(i: AttributeReference, IntegerLiteral(42))) =>

            assert(b.name == "b")
            assert(i.name == "i")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
    notMatchedActions(1) match {
      case InsertAction(None, assignments) =>
        assignments match {
          case Seq(
              Assignment(b: AttributeReference, BooleanLiteral(false)),
              Assignment(i: AttributeReference, IntegerLiteral(42))) =>

            assert(b.name == "b")
            assert(i.name == "i")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }

    notMatchedBySourceActions match {
      case Seq(UpdateAction(None, assignments)) =>
        assignments match {
          case Seq(
              Assignment(b: AttributeReference, bValue: AttributeReference),
              Assignment(i: AttributeReference, IntegerLiteral(42))) =>

            assert(b.name == "b" && b == bValue)
            assert(i.name == "i")

          case other =>
            fail(s"Unexpected assignments: $other")
        }

      case other =>
        fail(s"Unexpected actions: $other")
    }
  }

  private def parseAndAlignAssignments(
      query: String): (Seq[MergeAction], Seq[MergeAction], Seq[MergeAction]) = {

    parseAndResolve(query) match {
      case m: MergeIntoTable => (m.matchedActions, m.notMatchedActions, m.notMatchedBySourceActions)
      case plan => fail("Expected MergeIntoTable, but got:\n" + plan.treeString)
    }
  }
}
