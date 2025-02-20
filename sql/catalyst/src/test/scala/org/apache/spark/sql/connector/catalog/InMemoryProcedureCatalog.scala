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

import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.procedures.{BoundProcedure, ProcedureParameter, UnboundProcedure}
import org.apache.spark.sql.connector.read.{LocalScan, Scan}
import org.apache.spark.sql.types.{DataTypes, StructType}


class InMemoryProcedureCatalog extends InMemoryTableCatalog
  with ProcedureCatalog {

  protected val procedures: util.Map[Identifier, UnboundProcedure] =
    new util.HashMap[Identifier, UnboundProcedure]
  procedures.put(Identifier.of(Array("dummy"), "increment"), UnboundIncrement)

  override def loadProcedure(ident: Identifier): UnboundProcedure = {
    val procedure = procedures.get(ident)
    if (procedure == null) throw new RuntimeException("Procedure not found: " + ident)
      procedure
  }

  object UnboundIncrement extends UnboundProcedure {
    override def name: String = "increment"
    override def description: String = "test method to increment an in-memory counter"
    override def bind(inputType: StructType): BoundProcedure = BoundIncrement
  }

  object BoundIncrement extends BoundProcedure {
    private val value = new AtomicInteger(0)

    override def name: String = "dummy_increment"

    override def description: String = "test method to increment an in-memory counter"

    override def isDeterministic: Boolean = false

    override def parameters: Array[ProcedureParameter] = Array()

    def outputType: StructType = new StructType().add("out", DataTypes.IntegerType)

    override def call(input: InternalRow): java.util.Iterator[Scan] = {
      val result = Result(outputType, Array(InternalRow(value.incrementAndGet().intValue)))
      Collections.singleton[Scan](result).iterator()
    }
  }

  case class Result(readSchema: StructType, rows: Array[InternalRow]) extends LocalScan
}
