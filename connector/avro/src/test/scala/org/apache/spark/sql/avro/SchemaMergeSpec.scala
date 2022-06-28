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

package org.apache.spark.sql.avro

import scala.collection.JavaConverters._

import SchemaMerge.mergeSchemas
import org.apache.avro.JsonProperties.{NULL_VALUE => nullInstance}
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type._
import org.scalatest.funspec.AnyFunSpec

class SchemaMergeSpec extends AnyFunSpec {
  describe("merge") {
    it("should work") {
      // basic promotion
      assert(mergeSchemas(
        Schema.create(INT),
        Schema.create(LONG)
      ) ===
        Schema.create(LONG)
      )

      // order doesnt matter
      assert(mergeSchemas(
        Schema.create(LONG),
        Schema.create(INT)
      ) ===
        Schema.create(LONG)
      )

      // detecting nullable columns
      assert(mergeSchemas(
        Schema.create(LONG),
        Schema.createUnion(Schema.create(NULL), Schema.create(INT))
      ) ===
        Schema.createUnion(Schema.create(NULL), Schema.create(LONG))
      )

      // nullable columns without promotion
      assert(mergeSchemas(
        Schema.create(LONG),
        Schema.createUnion(Schema.create(NULL), Schema.create(STRING))
      ) ===
        Schema.createUnion(Schema.create(NULL), Schema.create(STRING), Schema.create(LONG))
      )

      // enums
      assert(mergeSchemas(
        Schema.createEnum("blah", null, "space1", Seq("x", "y").asJava),
        Schema.createEnum("blah", null, "space1", Seq("x1", "y1").asJava)
      ).getEnumSymbols.asScala ===
        Seq("x", "y", "x1", "y1")
      )

      // maps
      assert(mergeSchemas(
        Schema.createMap(Schema.create(STRING)),
        Schema.createMap(Schema.create(BYTES))
      ) ===
        Schema.createMap(Schema.create(BYTES))
      )

      // arrays
      assert(mergeSchemas(
        Schema.createArray(Schema.create(STRING)),
        Schema.createArray(Schema.create(BOOLEAN))
      ) ===
        Schema.createArray(Schema.createUnion(Schema.create(STRING), Schema.create(BOOLEAN)))
      )

      // records
      assert(mergeSchemas(
        Schema.createRecord(Seq(
          new Field("a", Schema.create(STRING), null, null),
          new Field("b", Schema.create(INT), null, null)
        ).asJava),
        Schema.createRecord(Seq(
          new Field("b", Schema.create(LONG), null, null),
          new Field("c", Schema.create(STRING), null, null)
        ).asJava)
      ) ===
        Schema.createRecord(Seq(
          new Field("a", Schema.createUnion(Schema.create(NULL), Schema.create(STRING)),
            null, nullInstance),
          new Field("b", Schema.create(LONG),
            null, null),
          new Field("c", Schema.createUnion(Schema.create(NULL), Schema.create(STRING)),
            null, nullInstance)
        ).asJava)
      )

      // hopeless
      assert(mergeSchemas(
        Schema.create(BOOLEAN),
        Schema.createMap(Schema.create(BOOLEAN))
      ) === Schema.createUnion(Schema.create(BOOLEAN), Schema.createMap(Schema.create(BOOLEAN))))

      // basic logical type
      assert(mergeSchemas(
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT)),
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT))
      ) ===
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT))
      )

      // logical type one side
      assert(mergeSchemas(
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT)),
        Schema.create(INT)
      ) ===
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT))
      )

      // logical type other side
      assert(mergeSchemas(
        Schema.create(INT),
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT))
      ) ===
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT))
      )

      // logical type and unions
      assert(mergeSchemas(
        LogicalTypes.timeMillis.addToSchema(Schema.create(INT)),
        Schema.createUnion(Schema.create(NULL), Schema.create(INT))
      ) ===
        Schema.createUnion(Schema.create(NULL),
          LogicalTypes.timeMillis.addToSchema(Schema.create(INT)))
      )
    }
  }
}
