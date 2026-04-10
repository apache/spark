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

import java.util.HashMap

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.analysis.resolver.{
  NameResolutionParameters,
  NameScope,
  NameScopeStack,
  NameTarget,
  SubqueryRegistry
}
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Attribute,
  AttributeReference,
  ExprId,
  GetArrayItem,
  GetArrayStructFields,
  GetMapValue,
  GetStructField,
  Literal,
  OuterReference
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class NameScopeSuite extends QueryTest with SharedSparkSession {
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
  private val col10Integer = AttributeReference(name = "col10", dataType = IntegerType)()
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
    val stack = newNameScopeStack()

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
    val stack = newNameScopeStack()

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

    val matchedMapStructs = stack.resolveMultipartName(Seq("col11", "key", "field"))
    assert(
      matchedMapStructs == NameTarget(
        candidates =
          Seq(GetStructField(GetMapValue(col11MapWithStruct, Literal("key")), 0, Some("field"))),
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
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer, col9NestedStruct, col10Map)))

    withSubqueryNameScope(stack) {
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
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))

    withSubqueryNameScope(stack) {
      withNameScope(stack) {
        withNameScope(stack) {
          withNameScope(stack) {
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

          withNameScope(stack) {
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
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))

    withSubqueryNameScope(stack) {
      stack.overwriteCurrent(output = Some(Seq(col1IntegerOther)))

      withSubqueryNameScope(stack) {
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
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col2Integer)),
      hiddenOutput = Some(Seq(col1Integer, col2Integer, col3Boolean))
    )

    assert(
      stack.resolveMultipartName(
        Seq("col3"),
        NameResolutionParameters(canResolveNameByHiddenOutput = false)
      ) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col1Integer, col2Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col3"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col3Boolean),
        output = Seq(col1Integer, col2Integer)
      )
    )
  }

  test("Hidden output gets properly propagated in a stack") {
    val stack = newNameScopeStack()

    withNameScope(stack) {

      stack.overwriteCurrent(output = Some(Seq(col1Integer)), hiddenOutput = Some(Seq(col1Integer)))

      withSubqueryNameScope(stack) {
        withNameScope(stack) {
          withNameScope(stack) {
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
            .resolveMultipartName(
              Seq("col3"),
              NameResolutionParameters(canResolveNameByHiddenOutput = true)
            ) == NameTarget(
            candidates = Seq(col3Boolean),
            output = Seq.empty
          )
        )

        stack.overwriteCurrent(output = Some(Seq(col1Integer)))
        assert(
          stack
            .resolveMultipartName(
              Seq("col3"),
              NameResolutionParameters(canResolveNameByHiddenOutput = true)
            ) == NameTarget(
            candidates = Seq(col3Boolean),
            output = Seq(col1Integer)
          )
        )
      }

      assert(stack.current.hiddenOutput == Seq(col1Integer))
      assert(
        stack
          .resolveMultipartName(
            Seq("col3"),
            NameResolutionParameters(canResolveNameByHiddenOutput = true)
          ) == NameTarget(
          candidates = Seq.empty,
          output = Seq(col1Integer)
        )
      )
    }
  }

  test("Hidden output gets prioritized because of conflict") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col1IntegerOther)),
      hiddenOutput = Some(Seq(col1IntegerOther, col2Integer)),
      availableAliases = Some({
        val map = new HashMap[ExprId, Alias]
        map.put(col1Integer.exprId, Alias(col1Integer, col1Integer.name)())
        map
      })
    )

    assert(
      stack.resolveMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer, col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(shouldPreferHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col1Integer, col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(
          canResolveNameByHiddenOutput = true,
          shouldPreferHiddenOutput = true
        )
      ) == NameTarget(
        candidates = Seq(col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
  }

  test("Main output gets prioritized because of conflict") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer)),
      hiddenOutput = Some(Seq(col1Integer, col1IntegerOther, col2Integer)),
      availableAliases = Some(new HashMap[ExprId, Alias])
    )

    assert(
      stack.resolveMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer),
        output = Seq(col1Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(shouldPreferHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col1Integer),
        output = Seq(col1Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col1Integer),
        output = Seq(col1Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(
          canResolveNameByHiddenOutput = true,
          shouldPreferHiddenOutput = true
        )
      ) == NameTarget(
        candidates = Seq(col1Integer),
        output = Seq(col1Integer)
      )
    )
  }

  test("Both main and hidden outputs have a conflict") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col1IntegerOther)),
      hiddenOutput = Some(Seq(col1Integer, col1IntegerOther, col2Integer)),
      availableAliases = Some(new HashMap[ExprId, Alias])
    )

    assert(
      stack.resolveMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer, col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(shouldPreferHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col1Integer, col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(col1Integer, col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1"),
        NameResolutionParameters(
          canResolveNameByHiddenOutput = true,
          shouldPreferHiddenOutput = true
        )
      ) == NameTarget(
        candidates = Seq(col1Integer, col1IntegerOther),
        output = Seq(col1Integer, col1IntegerOther)
      )
    )
  }

  test("Hidden output gets prioritized because of impossible extract") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col10Integer)),
      hiddenOutput = Some(Seq(col10Map)),
      availableAliases = Some({
        val map = new HashMap[ExprId, Alias]
        map.put(col10Integer.exprId, Alias(col10Integer, col10Integer.name)())
        map
      })
    )

    assert(
      stack.resolveMultipartName(Seq("col10", "key")) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col10Integer)
      )
    )
    assert(
      stack
        .resolveMultipartName(
          Seq("col10", "key"),
          NameResolutionParameters(shouldPreferHiddenOutput = true)
        ) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col10Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col10", "key"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(col10Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col10", "key"),
        NameResolutionParameters(
          canResolveNameByHiddenOutput = true,
          shouldPreferHiddenOutput = true
        )
      ) == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(col10Integer)
      )
    )
  }

  test("Main output gets prioritized because of impossible extract") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col10Map)),
      hiddenOutput = Some(Seq(col10Integer)),
      availableAliases = Some({
        val map = new HashMap[ExprId, Alias]
        map.put(col10Map.exprId, Alias(col10Map, col10Map.name)())
        map
      })
    )

    assert(
      stack.resolveMultipartName(Seq("col10", "key")) == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(col10Map)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col10", "key"),
        NameResolutionParameters(shouldPreferHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(col10Map)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col10", "key"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(col10Map)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col10", "key"),
        NameResolutionParameters(
          canResolveNameByHiddenOutput = true,
          shouldPreferHiddenOutput = true
        )
      ) == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        output = Seq(col10Map)
      )
    )
  }

  test("Both main and hidden outputs have impossible extract") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer)),
      hiddenOutput = Some(Seq(col1IntegerOther)),
      availableAliases = Some({
        val map = new HashMap[ExprId, Alias]
        map.put(col1Integer.exprId, Alias(col1Integer, col1Integer.name)())
        map
      })
    )

    assert(
      stack.resolveMultipartName(Seq("col1", "key")) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col1Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1", "key"),
        NameResolutionParameters(shouldPreferHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col1Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1", "key"),
        NameResolutionParameters(canResolveNameByHiddenOutput = true)
      ) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col1Integer)
      )
    )
    assert(
      stack.resolveMultipartName(
        Seq("col1", "key"),
        NameResolutionParameters(
          canResolveNameByHiddenOutput = true,
          shouldPreferHiddenOutput = true
        )
      ) == NameTarget(
        candidates = Seq.empty,
        output = Seq(col1Integer)
      )
    )
  }

  test("Empty stack") {
    val stack = newNameScopeStack()

    assert(stack.current.output.isEmpty)
  }

  test("Overwrite current with empty sequence") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq.empty))
    assert(stack.current.output == Seq.empty)
  }

  test("Overwrite top of the stack containing single scope") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq(col1Integer, col2Integer)))
    assert(stack.current.output == Seq(col1Integer, col2Integer))

    stack.overwriteCurrent(output = Some(Seq(col3Boolean, col4String)))
    assert(stack.current.output == Seq(col3Boolean, col4String))

    stack.overwriteCurrent(output = Some(Seq(col2Integer)))
    assert(stack.current.output == Seq(col2Integer))
  }

  test("Overwrite top of the stack containing several scopes") {
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq(col3Boolean)))

    val output = withNameScope(stack) {
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
    val stack = newNameScopeStack()

    stack.overwriteCurrent(output = Some(Seq(col1Integer)))

    val output = withNameScope(stack) {
      stack.overwriteCurrent(output = Some(Seq(col2Integer)))

      val output = withNameScope(stack) {
        stack.overwriteCurrent(output = Some(Seq(col3Boolean)))

        val output = withNameScope(stack) {
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

  test("availableAliasesNameLookup and availableAliases stay in sync after overwriting NameScope") {
    val stack = newNameScopeStack()

    val alias1 = Alias(col1Integer, "alias1")()
    val alias2 = Alias(col2Integer, "alias2")()
    val alias3 = Alias(col3Boolean, "alias3")()

    val aliasMap = new HashMap[ExprId, Alias]
    aliasMap.put(alias1.exprId, alias1)
    aliasMap.put(alias2.exprId, alias2)

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col2Integer)),
      availableAliases = Some(aliasMap)
    )

    assert(stack.current.hasAvailableAliasWithName("alias1"))
    assert(stack.current.hasAvailableAliasWithName("alias2"))
    assert(!stack.current.hasAvailableAliasWithName("alias3"))

    stack.current.registerAlias(alias3)

    assert(stack.current.hasAvailableAliasWithName("alias1"))
    assert(stack.current.hasAvailableAliasWithName("alias2"))
    assert(stack.current.hasAvailableAliasWithName("alias3"))

    val newAliasMap = new HashMap[ExprId, Alias]
    newAliasMap.put(alias1.exprId, alias1)
    newAliasMap.put(alias3.exprId, alias3)

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col3Boolean)),
      availableAliases = Some(newAliasMap)
    )

    assert(
      stack.current.hasAvailableAliasWithName("alias1"),
      "alias1 should be available after overwrite with constructor aliases"
    )
    assert(
      !stack.current.hasAvailableAliasWithName("alias2"),
      "alias2 should not be available after being removed from availableAliases"
    )
    assert(
      stack.current.hasAvailableAliasWithName("alias3"),
      "alias3 should be available after overwrite with constructor aliases"
    )
  }

  test("availableAliasesNameLookup case sensitivity") {
    val stack = newNameScopeStack()

    val aliasLowerCase = Alias(col1Integer, "myalias")()
    val aliasUpperCase = Alias(col2Integer, "MYALIAS")()
    val aliasMixedCase = Alias(col3Boolean, "MyAlias")()
    val aliasCamelCase = Alias(col4String, "myAliasName")()

    val aliasMap = new HashMap[ExprId, Alias]
    aliasMap.put(aliasLowerCase.exprId, aliasLowerCase)
    aliasMap.put(aliasUpperCase.exprId, aliasUpperCase)
    aliasMap.put(aliasMixedCase.exprId, aliasMixedCase)
    aliasMap.put(aliasCamelCase.exprId, aliasCamelCase)

    stack.overwriteCurrent(
      output = Some(Seq(col1Integer, col2Integer, col3Boolean, col4String)),
      availableAliases = Some(aliasMap)
    )

    assert(
      stack.current.hasAvailableAliasWithName("myalias"),
      "Should find alias with exact lowercase match"
    )
    assert(
      stack.current.hasAvailableAliasWithName("MYALIAS"),
      "Should find alias with uppercase query"
    )
    assert(
      stack.current.hasAvailableAliasWithName("MyAlias"),
      "Should find alias with mixed case query"
    )
    assert(
      stack.current.hasAvailableAliasWithName("myALIAS"),
      "Should find alias with different mixed case query"
    )

    assert(
      stack.current.hasAvailableAliasWithName("myAliasName"),
      "Should find camelCase alias with exact match"
    )
    assert(
      stack.current.hasAvailableAliasWithName("MYALIASNAME"),
      "Should find camelCase alias with uppercase query"
    )
    assert(
      stack.current.hasAvailableAliasWithName("myaliasname"),
      "Should find camelCase alias with lowercase query"
    )
    assert(
      stack.current.hasAvailableAliasWithName("MyAliasName"),
      "Should find camelCase alias with PascalCase query"
    )

    assert(
      !stack.current.hasAvailableAliasWithName("nonExistentAlias"),
      "Should not find non-existent alias"
    )
    assert(
      !stack.current.hasAvailableAliasWithName("NONEXISTENTALIAS"),
      "Should not find non-existent alias even with different case"
    )

    val newAlias = Alias(col1Integer, "TestCaseAlias")()
    stack.current.registerAlias(newAlias)

    assert(
      stack.current.hasAvailableAliasWithName("TestCaseAlias"),
      "Should find newly registered alias with exact case"
    )
    assert(
      stack.current.hasAvailableAliasWithName("testcasealias"),
      "Should find newly registered alias with lowercase"
    )
    assert(
      stack.current.hasAvailableAliasWithName("TESTCASEALIAS"),
      "Should find newly registered alias with uppercase"
    )
    assert(
      stack.current.hasAvailableAliasWithName("testCaseAlias"),
      "Should find newly registered alias with camelCase variation"
    )
  }

  private def newNameScopeStack() = new NameScopeStack(
    tempVariableManager = spark.sessionState.analyzer.catalogManager.tempVariableManager,
    subqueryRegistry = new SubqueryRegistry
  )

  private def withSubqueryNameScope[R](stack: NameScopeStack)(body: => R): R =
    withNameScopeImpl(stack, isSubqueryRoot = true) {
      body
    }

  private def withNameScope[R](stack: NameScopeStack)(body: => R): R =
    withNameScopeImpl(stack, isSubqueryRoot = false) {
      body
    }

  private def withNameScopeImpl[R](stack: NameScopeStack, isSubqueryRoot: Boolean)(
      body: => R): R = {
    stack.pushScope(isSubqueryRoot = isSubqueryRoot)
    try {
      body
    } finally {
      stack.popScope()
    }
  }
}
