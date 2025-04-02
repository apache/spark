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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.constraints.Constraint
import org.apache.spark.sql.connector.catalog.constraints.Constraint.ValidationStatus
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue, NamedReference}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types.IntegerType

class ConstraintSuite extends SparkFunSuite {

  test("CHECK constraint toDDL") {
    val con1 = Constraint.check("con1")
      .predicateSql("id > 10")
      .enforced(true)
      .validationStatus(ValidationStatus.VALID)
      .rely(true)
      .build()
    assert(con1.toDDL == "CONSTRAINT con1 CHECK (id > 10) ENFORCED VALID RELY")

    val con2 = Constraint.check("con2")
    .predicate(
      new Predicate(
        "=",
        Array[Expression](
          FieldReference(Seq("a", "b.c", "d")),
          LiteralValue(1, IntegerType))))
      .enforced(false)
      .validationStatus(ValidationStatus.VALID)
      .rely(true)
      .build()
    assert(con2.toDDL == "CONSTRAINT con2 CHECK (a.`b.c`.d = 1) NOT ENFORCED VALID RELY")

    val con3 = Constraint.check("con3")
      .predicateSql("a.b.c <=> 1")
      .predicate(
        new Predicate(
          "<=>",
          Array[Expression](
            FieldReference(Seq("a", "b", "c")),
            LiteralValue(1, IntegerType))))
      .enforced(false)
      .validationStatus(ValidationStatus.INVALID)
      .rely(false)
      .build()
    assert(con3.toDDL == "CONSTRAINT con3 CHECK (a.b.c <=> 1) NOT ENFORCED INVALID NORELY")

    val con4 = Constraint.check("con4").predicateSql("a = 1").build()
    assert(con4.toDDL == "CONSTRAINT con4 CHECK (a = 1) ENFORCED UNVALIDATED NORELY")
  }

  test("UNIQUE constraint toDDL") {
    val con1 = Constraint.unique(
        "con1",
        Array[NamedReference](FieldReference(Seq("a", "b", "c")), FieldReference(Seq("d"))))
      .enforced(false)
      .validationStatus(ValidationStatus.UNVALIDATED)
      .rely(true)
      .build()
    assert(con1.toDDL == "CONSTRAINT con1 UNIQUE (a.b.c, d) NOT ENFORCED UNVALIDATED RELY")

    val con2 = Constraint.unique(
        "con2",
        Array[NamedReference](FieldReference(Seq("a.b", "x", "y")), FieldReference(Seq("d"))))
      .enforced(false)
      .validationStatus(ValidationStatus.VALID)
      .rely(true)
      .build()
    assert(con2.toDDL == "CONSTRAINT con2 UNIQUE (`a.b`.x.y, d) NOT ENFORCED VALID RELY")
  }

  test("PRIMARY KEY constraint toDDL") {
    val pk1 = Constraint.primaryKey(
        "pk1",
        Array[NamedReference](FieldReference(Seq("a", "b", "c")), FieldReference(Seq("d"))))
      .enforced(true)
      .validationStatus(ValidationStatus.VALID)
      .rely(true)
      .build()
    assert(pk1.toDDL == "CONSTRAINT pk1 PRIMARY KEY (a.b.c, d) ENFORCED VALID RELY")

    val pk2 = Constraint.primaryKey(
        "pk2",
        Array[NamedReference](FieldReference(Seq("x.y", "z")), FieldReference(Seq("id"))))
      .enforced(false)
      .validationStatus(ValidationStatus.INVALID)
      .rely(false)
      .build()
    assert(pk2.toDDL == "CONSTRAINT pk2 PRIMARY KEY (`x.y`.z, id) NOT ENFORCED INVALID NORELY")
  }

  test("FOREIGN KEY constraint toDDL") {
    val fk1 = Constraint.foreignKey(
        "fk1",
        Array[NamedReference](FieldReference(Seq("col1")), FieldReference(Seq("col2"))),
        Identifier.of(Array("schema"), "table"),
        Array[NamedReference](FieldReference(Seq("ref_col1")), FieldReference(Seq("ref_col2"))))
      .enforced(true)
      .validationStatus(ValidationStatus.VALID)
      .rely(true)
      .build()
    assert(fk1.toDDL == "CONSTRAINT fk1 FOREIGN KEY (col1, col2) " +
      "REFERENCES schema.table (ref_col1, ref_col2) " +
      "ENFORCED VALID RELY")

    val fk2 = Constraint.foreignKey(
        "fk2",
        Array[NamedReference](FieldReference(Seq("x.y", "z"))),
        Identifier.of(Array.empty[String], "other_table"),
        Array[NamedReference](FieldReference(Seq("other_id"))))
      .enforced(false)
      .validationStatus(ValidationStatus.INVALID)
      .rely(false)
      .build()
    assert(fk2.toDDL == "CONSTRAINT fk2 FOREIGN KEY (`x.y`.z) " +
      "REFERENCES other_table (other_id) " +
      "NOT ENFORCED INVALID NORELY")
  }
}
