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

package org.apache.spark.sql.hive.execution

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.{BooleanType, DataType}

/**
 * The Hive table scan operator.  Column and partition pruning are both handled.
 *
 * @param requestedAttributes Attributes to be fetched from the Hive table.
 * @param relation The Hive table be be scanned.
 * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
 */
private[hive]
case class HiveTableScan(
    requestedAttributes: Seq[Attribute],
    relation: MetastoreRelation,
    partitionPruningPred: Option[Expression])(
    @transient val context: HiveContext)
  extends LeafNode {

  require(partitionPruningPred.isEmpty || relation.hiveQlTable.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")

  // Retrieve the original attributes based on expression ID so that capitalization matches.
  val attributes = requestedAttributes.map(relation.attributeMap)

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private[this] val boundPruningPred = partitionPruningPred.map { pred =>
    require(
      pred.dataType == BooleanType,
      s"Data type of predicate $pred must be BooleanType rather than ${pred.dataType}.")

    BindReferences.bindReference(pred, relation.partitionKeys)
  }

  // Create a local copy of hiveconf,so that scan specific modifications should not impact 
  // other queries
  @transient
  private[this] val hiveExtraConf = new HiveConf(context.hiveconf)

  // append columns ids and names before broadcast
  addColumnMetadataToConf(hiveExtraConf)

  @transient
  private[this] val hadoopReader = 
    new HadoopTableReader(attributes, relation, context, hiveExtraConf)

  private[this] def castFromString(value: String, dataType: DataType) = {
    Cast(Literal(value), dataType).eval(null)
  }

  private def addColumnMetadataToConf(hiveConf: HiveConf) {
    // Specifies needed column IDs for those non-partitioning columns.
    val neededColumnIDs = attributes.flatMap(relation.columnOrdinals.get).map(o => o: Integer)

    HiveShim.appendReadColumns(hiveConf, neededColumnIDs, attributes.map(_.name))

    val tableDesc = relation.tableDesc
    val deserializer = tableDesc.getDeserializerClass.newInstance
    deserializer.initialize(hiveConf, tableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, relation.attributes.map(_.name).mkString(","))
  }

  /**
   * Prunes partitions not involve the query plan.
   *
   * @param partitions All partitions of the relation.
   * @return Partitions that are involved in the query plan.
   */
  private[hive] def prunePartitions(partitions: Seq[HivePartition]) = {
    boundPruningPred match {
      case None => partitions
      case Some(shouldKeep) => partitions.filter { part =>
        val dataTypes = relation.partitionKeys.map(_.dataType)
        val castedValues = for ((value, dataType) <- part.getValues.zip(dataTypes)) yield {
          castFromString(value, dataType)
        }

        // Only partitioned values are needed here, since the predicate has already been bound to
        // partition key attribute references.
        val row = new GenericRow(castedValues.toArray)
        shouldKeep.eval(row).asInstanceOf[Boolean]
      }
    }
  }

  override def execute() = if (!relation.hiveQlTable.isPartitioned) {
    hadoopReader.makeRDDForTable(relation.hiveQlTable)
  } else {
    hadoopReader.makeRDDForPartitionedTable(prunePartitions(relation.hiveQlPartitions))
  }

  override def output = attributes
}
