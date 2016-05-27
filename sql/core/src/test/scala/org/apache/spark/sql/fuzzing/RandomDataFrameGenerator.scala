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

package org.apache.spark.sql.fuzzing

import java.util.concurrent.atomic.AtomicInteger

import scala.util.Random

import org.apache.spark.sql._
import org.apache.spark.sql.types._

class RandomDataFrameGenerator(
    seed: Long,
    @transient val sqlContext: SQLContext)
  extends Serializable {

  private val rand = new Random(seed)
  private val nextId = new AtomicInteger()

  private def hasRandomDataGenerator(dataType: DataType): Boolean = {
    RandomDataGenerator.forType(dataType).isDefined
  }

  def randomChoice[T](values: Seq[T]): T = {
    values(rand.nextInt(values.length))
  }

  private val simpleTypes: Set[DataType] = {
    DataTypeTestUtils.atomicTypes
      .filter(hasRandomDataGenerator)
      // Ignore decimal type since it can lead to OOM (see SPARK-9303). TODO: It would be better to
      // only generate limited precision decimals instead.
      .filterNot(_.isInstanceOf[DecimalType])
  }

  private val arrayTypes: Set[DataType] = {
    DataTypeTestUtils.atomicArrayTypes
      .filter(hasRandomDataGenerator)
      // Filter until SPARK-10038 is fixed.
      .filterNot(_.elementType.isInstanceOf[BinaryType])
      // See above comment about DecimalType
      .filterNot(_.elementType.isInstanceOf[DecimalType]).toSet
  }

  private def randomStructField(
      allowComplexTypes: Boolean = false,
      allowSpacesInColumnName: Boolean = false): StructField = {
    val name = "c" + nextId.getAndIncrement + (if (allowSpacesInColumnName) " space" else "")
    val candidateTypes: Seq[DataType] = Seq(
      simpleTypes,
      arrayTypes.filter(_ => allowComplexTypes),
      // This does not allow complex types, limiting the depth of recursion:
      if (allowComplexTypes) {
        Set[DataType](randomStructType(numCols = rand.nextInt(2) + 1))
      } else {
        Set[DataType]()
      }
    ).flatten
    val dataType = randomChoice(candidateTypes)
    val nullable = rand.nextBoolean()
    StructField(name, dataType, nullable)
  }

  private def randomStructType(
      numCols: Int,
      allowComplexTypes: Boolean = false,
      allowSpacesInColumnNames: Boolean = false): StructType = {
    StructType(Array.fill(numCols)(randomStructField(allowComplexTypes, allowSpacesInColumnNames)))
  }

  def randomDataFrame(
      numCols: Int,
      numRows: Int,
      allowComplexTypes: Boolean = false,
      allowSpacesInColumnNames: Boolean = false): DataFrame = {
    val schema = randomStructType(numCols, allowComplexTypes, allowSpacesInColumnNames)
    val rows = sqlContext.sparkContext.parallelize(1 to numRows).mapPartitions { iter =>
      val rowGenerator = RandomDataGenerator.forType(schema, nullable = false, rand = rand).get
      iter.map(_ => rowGenerator().asInstanceOf[Row])
    }
    sqlContext.createDataFrame(rows, schema)
  }

}
