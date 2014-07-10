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

import org.apache.hadoop.hive.common.`type`.{HiveDecimal, HiveVarchar}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition}
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.types.{BooleanType, DataType}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive._
import org.apache.spark.util.MutablePair

/**
 * :: DeveloperApi ::
 * The Hive table scan operator.  Column and partition pruning are both handled.
 *
 * @param attributes Attributes to be fetched from the Hive table.
 * @param relation The Hive table be be scanned.
 * @param partitionPruningPred An optional partition pruning predicate for partitioned table.
 */
@DeveloperApi
case class HiveTableScan(
    attributes: Seq[Attribute],
    relation: MetastoreRelation,
    partitionPruningPred: Option[Expression])(
    @transient val context: HiveContext)
  extends LeafNode
  with HiveInspectors {

  require(partitionPruningPred.isEmpty || relation.hiveQlTable.isPartitioned,
    "Partition pruning predicates only supported for partitioned tables.")

  // Bind all partition key attribute references in the partition pruning predicate for later
  // evaluation.
  private[this] val boundPruningPred = partitionPruningPred.map { pred =>
    require(
      pred.dataType == BooleanType,
      s"Data type of predicate $pred must be BooleanType rather than ${pred.dataType}.")

    BindReferences.bindReference(pred, relation.partitionKeys)
  }

  @transient
  private[this] val hadoopReader = new HadoopTableReader(relation.tableDesc, context)

  /**
   * The hive object inspector for this table, which can be used to extract values from the
   * serialized row representation.
   */
  @transient
  private[this] lazy val objectInspector =
    relation.tableDesc.getDeserializer.getObjectInspector.asInstanceOf[StructObjectInspector]

  /**
   * Functions that extract the requested attributes from the hive output.  Partitioned values are
   * casted from string to its declared data type.
   */
  @transient
  protected lazy val attributeFunctions: Seq[(Any, Array[String]) => Any] = {
    attributes.map { a =>
      val ordinal = relation.partitionKeys.indexOf(a)
      if (ordinal >= 0) {
        val dataType = relation.partitionKeys(ordinal).dataType
        (_: Any, partitionKeys: Array[String]) => {
          castFromString(partitionKeys(ordinal), dataType)
        }
      } else {
        val ref = objectInspector.getAllStructFieldRefs
          .find(_.getFieldName == a.name)
          .getOrElse(sys.error(s"Can't find attribute $a"))
        val fieldObjectInspector = ref.getFieldObjectInspector

        val unwrapHiveData = fieldObjectInspector match {
          case _: HiveVarcharObjectInspector =>
            (value: Any) => value.asInstanceOf[HiveVarchar].getValue
          case _: HiveDecimalObjectInspector =>
            (value: Any) => BigDecimal(value.asInstanceOf[HiveDecimal].bigDecimalValue())
          case _ =>
            identity[Any] _
        }

        (row: Any, _: Array[String]) => {
          val data = objectInspector.getStructFieldData(row, ref)
          val hiveData = unwrapData(data, fieldObjectInspector)
          if (hiveData != null) unwrapHiveData(hiveData) else null
        }
      }
    }
  }

  private[this] def castFromString(value: String, dataType: DataType) = {
    Cast(Literal(value), dataType).eval(null)
  }

  private def addColumnMetadataToConf(hiveConf: HiveConf) {
    // Specifies IDs and internal names of columns to be scanned.
    val neededColumnIDs = attributes.map(a => relation.output.indexWhere(_.name == a.name): Integer)
    val columnInternalNames = neededColumnIDs.map(HiveConf.getColumnInternalName(_)).mkString(",")

    if (attributes.size == relation.output.size) {
      ColumnProjectionUtils.setFullyReadColumns(hiveConf)
    } else {
      ColumnProjectionUtils.appendReadColumnIDs(hiveConf, neededColumnIDs)
    }

    ColumnProjectionUtils.appendReadColumnNames(hiveConf, attributes.map(_.name))

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        relation.tableDesc.getDeserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, columnInternalNames)
  }

  addColumnMetadataToConf(context.hiveconf)

  private def inputRdd = if (!relation.hiveQlTable.isPartitioned) {
    hadoopReader.makeRDDForTable(relation.hiveQlTable)
  } else {
    hadoopReader.makeRDDForPartitionedTable(prunePartitions(relation.hiveQlPartitions))
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

  override def execute() = {
    inputRdd.mapPartitions { iterator =>
      if (iterator.isEmpty) {
        Iterator.empty
      } else {
        val mutableRow = new GenericMutableRow(attributes.length)
        val mutablePair = new MutablePair[Any, Array[String]]()
        val buffered = iterator.buffered

        // NOTE (lian): Critical path of Hive table scan, unnecessary FP style code and pattern
        // matching are avoided intentionally.
        val rowsAndPartitionKeys = buffered.head match {
          // With partition keys
          case _: Array[Any] =>
            buffered.map { case array: Array[Any] =>
              val deserializedRow = array(0)
              val partitionKeys = array(1).asInstanceOf[Array[String]]
              mutablePair.update(deserializedRow, partitionKeys)
            }

          // Without partition keys
          case _ =>
            val emptyPartitionKeys = Array.empty[String]
            buffered.map { deserializedRow =>
              mutablePair.update(deserializedRow, emptyPartitionKeys)
            }
        }

        rowsAndPartitionKeys.map { pair =>
          var i = 0
          while (i < attributes.length) {
            mutableRow(i) = attributeFunctions(i)(pair._1, pair._2)
            i += 1
          }
          mutableRow: Row
        }
      }
    }
  }

  override def output = attributes
}
