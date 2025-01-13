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
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.UnresolvedStar
import org.apache.spark.sql.catalyst.analysis.resolver.{NameScope, NameScopeStack, NameTarget}
import org.apache.spark.sql.catalyst.expressions.{
  AttributeReference,
  GetArrayItem,
  GetArrayStructFields,
  GetMapValue,
  GetStructField,
  Literal
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

class NameScopeSuite extends PlanTest with SQLConfHelper {
  private val col1Integer = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col1IntegerOther = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col2Integer = AttributeReference(name = "col2", dataType = IntegerType)()
  private val col3Boolean = AttributeReference(name = "col3", dataType = BooleanType)()
  private val col4String = AttributeReference(name = "col4", dataType = StringType)()
  private val col5String = AttributeReference(name = "col5", dataType = StringType)()
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

    assert(nameScope.getAllAttributes.isEmpty)

    assert(nameScope.matchMultipartName(Seq("col1")) == NameTarget(candidates = Seq.empty))
  }

  test("Single unnamed plan") {
    val nameScope = new NameScope

    nameScope += Seq(col1Integer, col2Integer, col3Boolean)

    assert(nameScope.getAllAttributes == Seq(col1Integer, col2Integer, col3Boolean))

    assert(
      nameScope.matchMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col2")) == NameTarget(
        candidates = Seq(col2Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col3")) == NameTarget(
        candidates = Seq(col3Boolean),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col4")) == NameTarget(
        candidates = Seq.empty,
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
  }

  test("Several unnamed plans") {
    val nameScope = new NameScope

    nameScope += Seq(col1Integer)
    nameScope += Seq(col2Integer, col3Boolean)
    nameScope += Seq(col4String)

    assert(nameScope.getAllAttributes == Seq(col1Integer, col2Integer, col3Boolean, col4String))

    assert(
      nameScope.matchMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col2")) == NameTarget(
        candidates = Seq(col2Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col3")) == NameTarget(
        candidates = Seq(col3Boolean),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col4")) == NameTarget(
        candidates = Seq(col4String),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col5")) == NameTarget(
        candidates = Seq.empty,
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
  }

  test("Single named plan") {
    val nameScope = new NameScope

    nameScope("table1") = Seq(col1Integer, col2Integer, col3Boolean)

    assert(nameScope.getAllAttributes == Seq(col1Integer, col2Integer, col3Boolean))

    assert(
      nameScope.matchMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col2")) == NameTarget(
        candidates = Seq(col2Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col3")) == NameTarget(
        candidates = Seq(col3Boolean),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col4")) == NameTarget(
        candidates = Seq.empty,
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean)
      )
    )
  }

  test("Several named plans") {
    val nameScope = new NameScope

    nameScope("table1") = Seq(col1Integer)
    nameScope("table2") = Seq(col2Integer, col3Boolean)
    nameScope("table2") = Seq(col4String)
    nameScope("table3") = Seq(col5String)

    assert(
      nameScope.getAllAttributes == Seq(
        col1Integer,
        col2Integer,
        col3Boolean,
        col4String,
        col5String
      )
    )

    assert(
      nameScope.matchMultipartName(Seq("col1")) == NameTarget(
        candidates = Seq(col1Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String, col5String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col2")) == NameTarget(
        candidates = Seq(col2Integer),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String, col5String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col3")) == NameTarget(
        candidates = Seq(col3Boolean),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String, col5String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col4")) == NameTarget(
        candidates = Seq(col4String),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String, col5String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col5")) == NameTarget(
        candidates = Seq(col5String),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String, col5String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col6")) == NameTarget(
        candidates = Seq.empty,
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String, col5String)
      )
    )
  }

  test("Named and unnamed plans with case insensitive comparison") {
    val col1Integer = AttributeReference(name = "Col1", dataType = IntegerType)()
    val col2Integer = AttributeReference(name = "col2", dataType = IntegerType)()
    val col3Boolean = AttributeReference(name = "coL3", dataType = BooleanType)()
    val col4String = AttributeReference(name = "Col4", dataType = StringType)()

    val nameScope = new NameScope

    nameScope("TaBle1") = Seq(col1Integer)
    nameScope("table2") = Seq(col2Integer, col3Boolean)
    nameScope += Seq(col4String)

    assert(nameScope.getAllAttributes == Seq(col1Integer, col2Integer, col3Boolean, col4String))

    assert(
      nameScope.matchMultipartName(Seq("cOL1")) == NameTarget(
        candidates = Seq(col1Integer.withName("cOL1")),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("CoL2")) == NameTarget(
        candidates = Seq(col2Integer.withName("CoL2")),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col3")) == NameTarget(
        candidates = Seq(col3Boolean.withName("col3")),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("COL4")) == NameTarget(
        candidates = Seq(col4String.withName("COL4")),
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
    assert(
      nameScope.matchMultipartName(Seq("col5")) == NameTarget(
        candidates = Seq.empty,
        allAttributes = Seq(col1Integer, col2Integer, col3Boolean, col4String)
      )
    )
  }

  test("Duplicate attribute names from one plan") {
    val nameScope = new NameScope

    nameScope("table1") = Seq(col1Integer, col1Integer)
    nameScope("table1") = Seq(col1IntegerOther)

    assert(nameScope.getAllAttributes == Seq(col1Integer, col1Integer, col1IntegerOther))

    nameScope.matchMultipartName(Seq("col1")) == NameTarget(
      candidates = Seq(col1Integer, col1IntegerOther)
    )
  }

  test("Duplicate attribute names from several plans") {
    val nameScope = new NameScope

    nameScope("table1") = Seq(col1Integer, col1IntegerOther)
    nameScope("table2") = Seq(col1Integer, col1IntegerOther)

    assert(
      nameScope.getAllAttributes == Seq(
        col1Integer,
        col1IntegerOther,
        col1Integer,
        col1IntegerOther
      )
    )

    nameScope.matchMultipartName(Seq("col1")) == NameTarget(
      candidates = Seq(
        col1Integer,
        col1IntegerOther,
        col1Integer,
        col1IntegerOther
      )
    )
  }

  test("Expand star") {
    val nameScope = new NameScope

    nameScope("table") =
      Seq(col6IntegerWithQualifier, col6IntegerOtherWithQualifier, col7StringWithQualifier)

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

    nameScope("table2") = Seq(col6IntegerWithQualifier)

    checkError(
      exception = intercept[AnalysisException](
        nameScope.expandStar(UnresolvedStar(Some(Seq("table2"))))
      ),
      condition = "INVALID_USAGE_OF_STAR_OR_REGEX",
      parameters = Map(
        "elem" -> "'*'",
        "prettyName" -> "query"
      )
    )
  }

  test("Multipart attribute names") {
    val nameScope = new NameScope

    nameScope("table") = Seq(col6IntegerWithQualifier)

    for (multipartIdentifier <- Seq(
        Seq("catalog", "database", "table", "col6"),
        Seq("database", "table", "col6"),
        Seq("table", "col6")
      )) {
      assert(
        nameScope.matchMultipartName(multipartIdentifier) == NameTarget(
          candidates = Seq(
            col6IntegerWithQualifier
          ),
          allAttributes = Seq(col6IntegerWithQualifier)
        )
      )
    }

    for (multipartIdentifier <- Seq(
        Seq("catalog.database.table", "col6"),
        Seq("`database`.`table`.`col6`"),
        Seq("table.col6")
      )) {
      assert(
        nameScope.matchMultipartName(multipartIdentifier) == NameTarget(
          candidates = Seq.empty,
          allAttributes = Seq(col6IntegerWithQualifier)
        )
      )
    }
  }

  test("Nested fields") {
    val nameScope = new NameScope

    nameScope("table") = Seq(
      col8Struct,
      col9NestedStruct,
      col10Map,
      col11MapWithStruct,
      col12Array,
      col13ArrayWithStruct
    )

    var matchedStructs = nameScope.matchMultipartName(Seq("col8", "field"))
    assert(
      matchedStructs == NameTarget(
        candidates = Seq(
          GetStructField(col8Struct, 0, Some("field"))
        ),
        aliasName = Some("field"),
        allAttributes = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    matchedStructs = nameScope.matchMultipartName(Seq("col9", "field", "subfield"))
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
        allAttributes = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    var matchedMaps = nameScope.matchMultipartName(Seq("col10", "key"))
    assert(
      matchedMaps == NameTarget(
        candidates = Seq(GetMapValue(col10Map, Literal("key"))),
        aliasName = Some("key"),
        allAttributes = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    matchedMaps = nameScope.matchMultipartName(Seq("col11", "key"))
    assert(
      matchedMaps == NameTarget(
        candidates = Seq(GetMapValue(col11MapWithStruct, Literal("key"))),
        aliasName = Some("key"),
        allAttributes = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    var matchedArrays = nameScope.matchMultipartName(Seq("col12", "element"))
    assert(
      matchedArrays == NameTarget(
        candidates = Seq(GetArrayItem(col12Array, Literal("element"))),
        aliasName = Some("element"),
        allAttributes = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    matchedArrays = nameScope.matchMultipartName(Seq("col13", "field"))
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
        allAttributes = Seq(
          col8Struct,
          col9NestedStruct,
          col10Map,
          col11MapWithStruct,
          col12Array,
          col13ArrayWithStruct
        )
      )
    )

    nameScope("table2") = Seq(col8Struct)
    matchedStructs = nameScope.matchMultipartName(Seq("col8", "field"))
    assert(
      matchedStructs == NameTarget(
        candidates = Seq(
          GetStructField(
            col8Struct,
            0,
            Some("field")
          ),
          GetStructField(
            col8Struct,
            0,
            Some("field")
          )
        ),
        aliasName = Some("field"),
        allAttributes = Seq(
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
}

class NameScopeStackSuite extends PlanTest {
  private val col1Integer = AttributeReference(name = "col1", dataType = IntegerType)()
  private val col2String = AttributeReference(name = "col2", dataType = StringType)()
  private val col3Integer = AttributeReference(name = "col3", dataType = IntegerType)()
  private val col4String = AttributeReference(name = "col4", dataType = StringType)()

  test("Empty stack") {
    val stack = new NameScopeStack

    assert(stack.top.getAllAttributes.isEmpty)
  }

  test("Overwrite top of the stack containing single scope") {
    val stack = new NameScopeStack

    stack.top.update("table1", Seq(col1Integer, col2String))
    assert(stack.top.getAllAttributes == Seq(col1Integer, col2String))

    stack.overwriteTop("table2", Seq(col3Integer, col4String))
    assert(stack.top.getAllAttributes == Seq(col3Integer, col4String))

    stack.overwriteTop(Seq(col2String))
    assert(stack.top.getAllAttributes == Seq(col2String))
  }

  test("Overwrite top of the stack containing several scopes") {
    val stack = new NameScopeStack

    stack.top.update("table2", Seq(col3Integer))

    stack.withNewScope {
      assert(stack.top.getAllAttributes.isEmpty)

      stack.top.update("table1", Seq(col1Integer, col2String))
      assert(stack.top.getAllAttributes == Seq(col1Integer, col2String))

      stack.overwriteTop("table2", Seq(col3Integer, col4String))
      assert(stack.top.getAllAttributes == Seq(col3Integer, col4String))

      stack.overwriteTop(Seq(col2String))
      assert(stack.top.getAllAttributes == Seq(col2String))
    }
  }

  test("Scope stacking") {
    val stack = new NameScopeStack

    stack.top.update("table1", Seq(col1Integer))

    stack.withNewScope {
      stack.top.update("table2", Seq(col2String))

      stack.withNewScope {
        stack.top.update("table3", Seq(col3Integer))

        stack.withNewScope {
          stack.top.update("table4", Seq(col4String))

          assert(stack.top.getAllAttributes == Seq(col4String))
        }

        assert(stack.top.getAllAttributes == Seq(col3Integer))
      }

      assert(stack.top.getAllAttributes == Seq(col2String))
    }

    assert(stack.top.getAllAttributes == Seq(col1Integer))
  }
}
