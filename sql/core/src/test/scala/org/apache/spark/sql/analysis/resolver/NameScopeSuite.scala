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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.analysis.resolver.{NameScope, NameScopeStack, NameTarget}
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  GetArrayItem,
  GetArrayStructFields,
  GetMapValue,
  GetStructField,
  Literal,
  OuterReference
}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.types.{
  ArrayType,
  BooleanType,
  IntegerType,
  MapType,
  StringType,
  StructField,
  StructType
}

class NameScopeSuite extends PlanTest {
  private val col1Integer = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col1IntegerOther = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col2Integer = AttributeReference(name = "col2", dataType = IntegerType)()
  private val col3Boolean = AttributeReference(name = "col3", dataType = BooleanType)()
  private val col4String = AttributeReference(name = "col4", dataType = StringType)()
  private val col6IntegerWithQualifier = AttributeReference(
    name = "col6",
    dataType = IntegerType
  )(qualifier = Seq("catalog", "database", "table"))
  private val col6IntegerOtherWithQualifier = AttributeReference(
    name = "col6",
    dataType = IntegerType
  )(qualifier = Seq("catalog", "database", "table"))
  private val col7StringWithQualifier = AttributeReference(
    name = "col7",
    dataType = IntegerType
  )(qualifier = Seq("catalog", "database", "table"))
  private val col8Struct = AttributeReference(
    name = "col8",
    dataType = StructType(Seq(StructField("field", IntegerType, true)))
  )()
  private val col9NestedStruct = AttributeReference(
    name = "col9",
    dataType = StructType(
      Seq(
        StructField(
          "field",
          StructType(
            Seq(
              StructField("subfield", IntegerType)
            )
          )
        )
      )
    )
  )()
  private val col10Map = AttributeReference(
    name = "col10",
    dataType = MapType(StringType, IntegerType)
  )()
  private val col11MapWithStruct = AttributeReference(
    name = "col11",
    dataType = MapType(
      StringType,
      StructType(Seq(StructField("field", StringType)))
    )
  )()
  private val col12Array = AttributeReference(
    name = "col12",
    dataType = ArrayType(IntegerType)
  )()
  private val col13ArrayWithStruct = AttributeReference(
    name = "col13",
    dataType = ArrayType(
      StructType(Seq(StructField("field", StringType)))
    )
  )()

  test("Empty scope") {
    val nameScope = new NameScope

    assert(nameScope.output.isEmpty)

    checkOnePartNameLookup(
      nameScope,
      name = "col1",
      candidates = Seq.empty
    )
  }

  test("Distinct attributes") {
    val nameScope = new NameScope(Seq(col1Integer, col2Integer, col3Boolean, col4String))

    assert(nameScope.output == Seq(col1Integer, col2Integer, col3Boolean, col4String))

    checkOnePartNameLookup(
      nameScope,
      name = "col1",
      candidates = Seq(col1Integer)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "col2",
      candidates = Seq(col2Integer)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "col3",
      candidates = Seq(col3Boolean)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "col4",
      candidates = Seq(col4String)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "col5",
      candidates = Seq.empty
    )
  }

  test("Duplicate attribute names") {
    val nameScope = new NameScope(Seq(col1Integer, col1Integer, col1IntegerOther))

    assert(nameScope.output == Seq(col1Integer, col1Integer, col1IntegerOther))

    checkOnePartNameLookup(
      nameScope,
      name = "col1",
      candidates = Seq(col1Integer, col1Integer, col1IntegerOther)
    )
  }

  test("Case insensitive comparison") {
    val col1Integer = AttributeReference(name = "Col1", dataType = IntegerType)()
    val col2Integer = AttributeReference(name = "col2", dataType = IntegerType)()
    val col3Boolean = AttributeReference(name = "coL3", dataType = BooleanType)()
    val col3BooleanOther = AttributeReference(name = "Col3", dataType = BooleanType)()
    val col4String = AttributeReference(name = "Col4", dataType = StringType)()

    val nameScope =
      new NameScope(
        Seq(col1Integer, col3Boolean, col2Integer, col2Integer, col3BooleanOther, col4String)
      )

    assert(
      nameScope.output == Seq(
        col1Integer,
        col3Boolean,
        col2Integer,
        col2Integer,
        col3BooleanOther,
        col4String
      )
    )

    checkOnePartNameLookup(
      nameScope,
      name = "cOL1",
      candidates = Seq(col1Integer)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "CoL2",
      candidates = Seq(col2Integer, col2Integer)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "col3",
      candidates = Seq(col3Boolean, col3BooleanOther)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "COL4",
      candidates = Seq(col4String)
    )
    checkOnePartNameLookup(
      nameScope,
      name = "col5",
      candidates = Seq.empty
    )
  }

  test("Expand star") {
    val nameScope = new NameScope(
      Seq(col6IntegerWithQualifier, col6IntegerOtherWithQualifier, col7StringWithQualifier)
    )

    Seq(Seq("table"), Seq("database", "table"), Seq("catalog", "database", "table"))
      .foreach(tableQualifier => {
        assert(
          nameScope.expandStar(UnresolvedStar(Some(tableQualifier)))
          == Seq(col6IntegerWithQualifier, col6IntegerOtherWithQualifier, col7StringWithQualifier)
        )
      })

    checkError(
      exception = intercept[AnalysisException](
        nameScope.expandStar(UnresolvedStar(Some(Seq("database", "table_fail"))))
      ),
      condition = "CANNOT_RESOLVE_STAR_EXPAND",
      parameters = Map(
        "targetString" -> "`database`.`table_fail`",
        "columns" -> "`col6`, `col6`, `col7`"
      )
    )
  }

  test("Multipart attribute names") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col6IntegerWithQualifier)))

    for (multipartIdentifier <- Seq(
        Seq("catalog", "database", "table", "col6"),
        Seq("database", "table", "col6"),
        Seq("table", "col6")
      )) {
      assert(
        stack.resolveMultipartName(multipartIdentifier) == NameTarget(
          candidates = Seq(col6IntegerWithQualifier),
          output = Seq(col6IntegerWithQualifier)
        )
      )
    }

    for (multipartIdentifier <- Seq(
        Seq("catalog.database.table", "col6"),
        Seq("`database`.`table`.`col6`"),
        Seq("table.col6")
      )) {
      assert(
        stack.resolveMultipartName(multipartIdentifier) == NameTarget(
          candidates = Seq.empty,
          output = Seq(col6IntegerWithQualifier)
        )
      )
    }
  }

  test("Nested fields") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(
      output = Some(
        Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    var matchedStructs = stack.resolveMultipartName(Seq("col8", "field"))
    assert(
      matchedStructs == NameTarget(
        candidates = Seq(
          GetStructField(col8Struct, 0, Some("field"))
        ),
        aliasName = Some("field"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    matchedStructs = stack.resolveMultipartName(Seq("col9", "field", "subfield"))
    assert(
      matchedStructs == NameTarget(
        candidates = Seq(
          GetStructField(
            GetStructField(
              col9NestedStruct,
              0,
              Some("field")
            ),
            0,
            Some("subfield")
          )
        ),
        aliasName = Some("subfield"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    var matchedMaps = stack.resolveMultipartName(Seq("col10", "key"))
    assert(
      matchedMaps == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    matchedMaps = stack.resolveMultipartName(Seq("col11", "key"))
    assert(
      matchedMaps == NameTarget(
        candidates = Seq(GetMapValue(col11MapWithStruct, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    var matchedArrays = stack.resolveMultipartName(Seq("col12", "element"))
    assert(
      matchedArrays == NameTarget(
        candidates = Seq(GetArrayItem(col12Array, Literal("element"))),
        aliasName = Some("element"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    matchedArrays = stack.resolveMultipartName(Seq("col13", "field"))
    assert(
      matchedArrays == NameTarget(
        candidates = Seq(
          GetArrayStructFields(
            col13ArrayWithStruct,
            StructField("field", StringType, true),
            0,
            1,
            true
          )
        ),
        aliasName = Some("field"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    stack.overwriteCurrent(output = Some(stack.current.output :+ col8Struct))

    matchedStructs = stack.resolveMultipartName(Seq("col8", "field"))
    assert(
      matchedStructs == NameTarget(
        candidates = Seq(
          GetStructField(
            col8Struct,
            0,
            Some("field")
          )
        ),
        aliasName = Some("field"),
        output = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct,
          col8Struct
        )
      )
    )
  }

  test("Direct outer references") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer, col9NestedStruct, col10Map)))

    stack.withNewScope(isSubqueryRoot = true) {
      stack.overwriteCurrent(output = Some(Seq(col1IntegerOther, col3Boolean)))

      assert(
        stack.resolveMultipartName(Seq("col0")) == NameTarget(
          candidates = Seq.empty,
          output = Seq(col1IntegerOther, col3Boolean)
        )
      )
      assert(
        stack.resolveMultipartName(Seq("col1")) == NameTarget(
          candidates = Seq(col1IntegerOther),
          output = Seq(col1IntegerOther, col3Boolean)
        )
      )
      assert(
        stack.resolveMultipartName(Seq("col2")) == NameTarget(
          candidates = Seq(OuterReference(col2Integer)),
          output = Seq(col1Integer, col2Integer, col9NestedStruct, col10Map),
          isOuterReference = true
        )
      )
      assert(
        stack.resolveMultipartName(Seq("col3")) == NameTarget(
          candidates = Seq(col3Boolean),
          output = Seq(col1IntegerOther, col3Boolean)
        )
      )
      assert(
        stack.resolveMultipartName(Seq("col9", "field", "subfield")) == NameTarget(
          candidates = Seq(
            GetStructField(
              GetStructField(
                OuterReference(col9NestedStruct),
                0,
                Some("field")
              ),
              0,
              Some("subfield")
            )
          ),
          aliasName = Some("subfield"),
          output = Seq(col1Integer, col2Integer, col9NestedStruct, col10Map),
          isOuterReference = true
        )
      )

      assert(
        stack.resolveMultipartName(Seq("col10", "key")) == NameTarget(
          candidates = Seq(GetMapValue(OuterReference(col10Map), Literal("key"))),
          aliasName = Some("key"),
          output = Seq(col1Integer, col2Integer, col9NestedStruct, col10Map),
          isOuterReference = true
        )
      )
    }
  }

  test("Outer references through layers of scopes") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))

    stack.withNewScope(isSubqueryRoot = true) {
      stack.withNewScope() {
        stack.withNewScope() {
          stack.withNewScope() {
            assert(
              stack.resolveMultipartName(Seq("col1")) == NameTarget(
                candidates = Seq(OuterReference(col1Integer)),
                output = Seq(col1Integer, col2Integer),
                isOuterReference = true
              )
            )
            assert(
              stack.resolveMultipartName(Seq("col2")) == NameTarget(
                candidates = Seq(OuterReference(col2Integer)),
                output = Seq(col1Integer, col2Integer),
                isOuterReference = true
              )
            )
          }

          stack.overwriteCurrent(output = Some(Seq(col1IntegerOther, col3Boolean)))

          stack.withNewScope() {
            assert(
              stack.resolveMultipartName(Seq("col1")) == NameTarget(
                candidates = Seq(OuterReference(col1Integer)),
                output = Seq(col1Integer, col2Integer),
                isOuterReference = true
              )
            )
            assert(
              stack.resolveMultipartName(Seq("col2")) == NameTarget(
                candidates = Seq(OuterReference(col2Integer)),
                output = Seq(col1Integer, col2Integer),
                isOuterReference = true
              )
            )
          }
        }
      }
    }
  }

  test("Nested correlation") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))

    stack.withNewScope(isSubqueryRoot = true) {
      stack.overwriteCurrent(output = Some(Seq(col1IntegerOther)))

      stack.withNewScope(isSubqueryRoot = true) {
        stack.overwriteCurrent(output = Some(Seq(col3Boolean)))

        assert(
          stack.resolveMultipartName(Seq("col1")) == NameTarget(
            candidates = Seq(OuterReference(col1IntegerOther)),
            output = Seq(col1IntegerOther),
            isOuterReference = true
          )
        )
        assert(
          stack.resolveMultipartName(Seq("col2")) == NameTarget(
            candidates = Seq.empty,
            output = Seq(col3Boolean)
          )
        )
        assert(
          stack.resolveMultipartName(Seq("col3")) == NameTarget(
            candidates = Seq(col3Boolean),
            output = Seq(col3Boolean)
          )
        )
      }
    }
  }

  test("Resolve attribute from hidden output") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col2Integer)),
      hiddenOutput = Some(Seq(col1Integer, col2Integer, col3Boolean))
    )

    assert(
      stack.resolveMultipartName(Seq("col3"), canResolveNameByHiddenOutput = false) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col1Integer, col2Integer)
      )
    )
    assert(
      stack.resolveMultipartName(Seq("col3"), canResolveNameByHiddenOutput = true) == NameTarget(
        candidates = Seq(col3Boolean),
        output = Seq(col1Integer, col2Integer)
      )
    )
  }

  test("Hidden output gets properly propagated in a stack") {
    val stack = new NameScopeStack

    stack.withNewScope() {

      stack.overwriteCurrent(output = Some(Seq(col1Integer)), hiddenOutput = Some(Seq(col1Integer)))

      stack.withNewScope(isSubqueryRoot = true) {

        stack.withNewScope() {
          stack.withNewScope() {
            stack.overwriteCurrent(
              output = Some(Seq(col1Integer, col2Integer)),
              hiddenOutput = Some(Seq(col1Integer, col2Integer, col3Boolean))
            )
          }
        }

        assert(
          stack.resolveMultipartName(Seq("col3")) == NameTarget(
            candidates = Seq.empty,
            output = Seq.empty
          )
        )
        assert(
          stack
            .resolveMultipartName(Seq("col3"), canResolveNameByHiddenOutput = true) == NameTarget(
            candidates = Seq(col3Boolean),
            output = Seq.empty
          )
        )

        stack.overwriteCurrent(output = Some(Seq(col1Integer)))
        assert(
          stack
            .resolveMultipartName(Seq("col3"), canResolveNameByHiddenOutput = true) == NameTarget(
            candidates = Seq(col3Boolean),
            output = Seq(col1Integer)
          )
        )
      }

      assert(stack.current.hiddenOutput == Seq(col1Integer))
      assert(
        stack
          .resolveMultipartName(Seq("col3"), canResolveNameByHiddenOutput = true) == NameTarget(
          candidates = Seq.empty,
          output = Seq(col1Integer)
        )
      )
    }
  }

  test("Empty stack") {
    val stack = new NameScopeStack

    assert(stack.current.output.isEmpty)
  }

  test("Overwrite current with empty sequence") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq.empty))
    assert(stack.current.output == Seq.empty)
  }

  test("Overwrite top of the stack containing single scope") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))
    assert(stack.current.output == Seq(col1Integer, col2Integer))

    stack.overwriteCurrent(output = Some(Seq(col3Boolean, col4String)))
    assert(stack.current.output == Seq(col3Boolean, col4String))

    stack.overwriteCurrent(output = Some(Seq(col2Integer)))
    assert(stack.current.output == Seq(col2Integer))
  }

  test("Overwrite top of the stack containing several scopes") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col3Boolean)))

    val output = stack.withNewScope() {
      assert(stack.current.output.isEmpty)

      stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))
      assert(stack.current.output == Seq(col1Integer, col2Integer))

      stack.overwriteCurrent(output = Some(Seq(col3Boolean, col4String)))
      assert(stack.current.output == Seq(col3Boolean, col4String))

      stack.overwriteCurrent(output = Some(Seq(col2Integer)))
      assert(stack.current.output == Seq(col2Integer))

      stack.current.output
    }

    assert(output == Seq(col2Integer))
  }

  test("Scope stacking") {
    val stack = new NameScopeStack

    stack.overwriteCurrent(output = Some(Seq(col1Integer)))

    val output = stack.withNewScope() {
      stack.overwriteCurrent(output = Some(Seq(col2Integer)))

      val output = stack.withNewScope() {
        stack.overwriteCurrent(output = Some(Seq(col3Boolean)))

        val output = stack.withNewScope() {
          stack.overwriteCurrent(output = Some(Seq(col4String)))

          assert(stack.current.output == Seq(col4String))

          stack.current.output
        }

        assert(output == Seq(col4String))
        assert(stack.current.output == Seq(col3Boolean))

        stack.current.output
      }

      assert(output == Seq(col3Boolean))
      assert(stack.current.output == Seq(col2Integer))

      stack.current.output
    }

    assert(output == Seq(col2Integer))
    assert(stack.current.output == Seq(col1Integer))
  }

  /**
   * Check both [[resolveMultipartName]] and [[findAttributesByName]] for a single part name.
   *
   * [[resolveMultipartName]] respects the case sensitivity of the input name, and candidates are
   * gonna have a new name which is case-identical to the queried name, while
   * [[findAttributesByName]] is just a simple case-insensitive lookup. Also,
   * [[resolveMultipartName]] deduplicates the candidates.
   */
  private def checkOnePartNameLookup(
      nameScope: NameScope,
      name: String,
      candidates: Seq[Attribute]): Unit = {
    assert(
      nameScope.resolveMultipartName(Seq(name)) == NameTarget(
        candidates = candidates.distinct.map(attribute => attribute.withName(name)),
        output = nameScope.output
      )
    )
    assert(nameScope.findAttributesByName(name) == candidates)
  }
}
