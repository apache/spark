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
package org.apache.spark.sql.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable, Put, Result, Scan}
import org.apache.hadoop.hbase.filter._
import org.apache.log4j.Logger

import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase.catalyst.NotPusher
import org.apache.spark.sql.hbase.types.PartitionRange
import org.apache.spark.sql.hbase.util.{DataTypeUtils, HBaseKVHelper, BytesUtils, Util}
import org.apache.spark.sql.sources.{BaseRelation, CatalystScan, LogicalRelation}
import org.apache.spark.sql.sources.{RelationProvider, InsertableRelation}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class HBaseSource extends RelationProvider {
  // Returns a new HBase relation with the given parameters
  override def createRelation(
                               sqlContext: SQLContext,
                               parameters: Map[String, String]): BaseRelation = {
    val catalog = sqlContext.catalog.asInstanceOf[HBaseCatalog]

    val tableName = parameters("tableName")
    val rawNamespace = parameters("namespace")
    val namespace: Option[String] = if (rawNamespace == null || rawNamespace.isEmpty) None
    else Some(rawNamespace)
    val hbaseTable = parameters("hbaseTableName")
    val colsSeq = parameters("colsSeq").split(",")
    val keyCols = parameters("keyCols").split(";")
      .map { case c => val cols = c.split(","); (cols(0), cols(1))}
    val nonKeyCols = parameters("nonKeyCols").split(";")
      .filterNot(_ == "")
      .map { case c => val cols = c.split(","); (cols(0), cols(1), cols(2), cols(3))}

    val keyMap: Map[String, String] = keyCols.toMap
    val allColumns = colsSeq.map {
      case name =>
        if (keyMap.contains(name)) {
          KeyColumn(
            name,
            catalog.getDataType(keyMap.get(name).get),
            keyCols.indexWhere(_._1 == name))
        } else {
          val nonKeyCol = nonKeyCols.find(_._1 == name).get
          NonKeyColumn(
            name,
            catalog.getDataType(nonKeyCol._2),
            nonKeyCol._3,
            nonKeyCol._4
          )
        }
    }
    catalog.createTable(tableName, rawNamespace, hbaseTable, allColumns, null)
  }
}

/**
 *
 * @param tableName SQL table name
 * @param hbaseNamespace physical HBase table namespace
 * @param hbaseTableName physical HBase table name
 * @param allColumns schema
 * @param context HBaseSQLContext
 */
@SerialVersionUID(15298736227428789L)
private[hbase] case class HBaseRelation(
     tableName: String,
     hbaseNamespace: String,
     hbaseTableName: String,
     allColumns: Seq[AbstractColumn])(@transient var context: SQLContext)
  extends BaseRelation with CatalystScan with InsertableRelation with Serializable {

  @transient lazy val logger = Logger.getLogger(getClass.getName)

  @transient lazy val keyColumns = allColumns.filter(_.isInstanceOf[KeyColumn])
    .asInstanceOf[Seq[KeyColumn]].sortBy(_.order)

  // The sorting is by the ordering of the Column Family and Qualifier. This is for avoidance
  // to sort cells per row, as needed in bulk loader
  @transient lazy val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    .asInstanceOf[Seq[NonKeyColumn]].sortWith(
      (a: NonKeyColumn, b: NonKeyColumn) => {
        val empty = new HBaseRawType(0)
        KeyValue.COMPARATOR.compare(
          new KeyValue(empty, a.familyRaw, a.qualifierRaw),
          new KeyValue(empty, b.familyRaw, b.qualifierRaw)) < 0
      }
    )

  lazy val partitionKeys = keyColumns.map(col =>
    logicalRelation.output.find(_.name == col.sqlName).get)

  @transient lazy val columnMap = allColumns.map {
    case key: KeyColumn => (key.sqlName, key.order)
    case nonKey: NonKeyColumn => (nonKey.sqlName, nonKey)
  }.toMap

  allColumns.zipWithIndex.foreach(pi => pi._1.ordinal = pi._2)

  private var serializedConfiguration: Array[Byte] = _

  def setConfig(inconfig: Configuration) = {
    config = inconfig
    if (inconfig != null) {
      serializedConfiguration = Util.serializeHBaseConfiguration(inconfig)
    }
  }

  @transient var config: Configuration = _

  private def getConf: Configuration = {
    if (config == null) {
      config = {
        if (serializedConfiguration != null) {
          Util.deserializeHBaseConfiguration(serializedConfiguration)
        }
        else {
          HBaseConfiguration.create
        }
      }
    }
    config
  }

  logger.debug(s"HBaseRelation config has zkPort="
    + s"${getConf.get("hbase.zookeeper.property.clientPort")}")

  @transient private var htable_ : HTable = _

  def htable = {
    if (htable_ == null) htable_ = new HTable(getConf, hbaseTableName)
    htable_
  }

  def isNonKey(attr: AttributeReference): Boolean = {
    keyIndex(attr) < 0
  }

  def keyIndex(attr: AttributeReference): Int = {
    // -1 if nonexistent
    partitionKeys.indexWhere(_.exprId == attr.exprId)
  }

  // find the index in a sequence of AttributeReferences that is a key; -1 if not present
  def rowIndex(refs: Seq[Attribute], keyIndex: Int): Int = {
    refs.indexWhere(_.exprId == partitionKeys(keyIndex).exprId)
  }

  def flushHTable() = {
    if (htable_ != null) {
      htable_.flushCommits()
    }
  }

  def closeHTable() = {
    if (htable_ != null) {
      htable_.close()
      htable_ = null
    }
  }

  // corresponding logical relation
  @transient lazy val logicalRelation = LogicalRelation(this)

  lazy val output = logicalRelation.output

  @transient lazy val dts: Seq[DataType] = allColumns.map(_.dataType)

  /**
   * partitions are updated per table lookup to keep the info reasonably updated
   */
  @transient lazy val partitionExpiration =
    context.conf.asInstanceOf[HBaseSQLConf].partitionExpiration * 1000
  @transient var partitionTS: Long = _

  private[hbase] def fetchPartitions(): Unit = {
    if (System.currentTimeMillis - partitionTS >= partitionExpiration) {
      partitionTS = System.currentTimeMillis
      partitions = {
        val regionLocations = htable.getRegionLocations.asScala.toSeq
        logger.info(s"Number of HBase regions for " +
          s"table ${htable.getName.getNameAsString}: ${regionLocations.size}")
        regionLocations.zipWithIndex.map {
          case p =>
            val start: Option[HBaseRawType] = {
              if (p._1._1.getStartKey.length == 0) {
                None
              } else {
                Some(p._1._1.getStartKey)
              }
            }
            val end: Option[HBaseRawType] = {
              if (p._1._1.getEndKey.length == 0) {
                None
              } else {
                Some(p._1._1.getEndKey)
              }
            }
            new HBasePartition(
              p._2, p._2,
              start,
              end,
              Some(p._1._2.getHostname), relation = this)
        }
      }
    }
  }

  @transient var partitions: Seq[HBasePartition] = _

  @transient private[hbase] lazy val dimSize = keyColumns.size

  val scannerFetchSize = context.conf.asInstanceOf[HBaseSQLConf].scannerFetchSize

  private[hbase] def generateRange(partition: HBasePartition, pred: Expression,
                                   index: Int): PartitionRange[_] = {
    def getData(dt: NativeType,
                buffer: ListBuffer[HBaseRawType],
                aBuffer: ArrayBuffer[Byte],
                bound: Option[HBaseRawType]): Option[Any] = {
      if (bound.isEmpty) None
      else {
        val (start, length) = HBaseKVHelper.decodingRawKeyColumns(bound.get, keyColumns)(index)
        Some(DataTypeUtils.bytesToData(bound.get, start, length, dt).asInstanceOf[dt.JvmType])
      }
    }

    val dt = keyColumns(index).dataType.asInstanceOf[NativeType]
    val isLastKeyIndex = index == (keyColumns.size - 1)
    val buffer = ListBuffer[HBaseRawType]()
    val aBuffer = ArrayBuffer[Byte]()
    val start = getData(dt, buffer, aBuffer, partition.start)
    val end = getData(dt, buffer, aBuffer, partition.end)
    val startInclusive = start.nonEmpty
    val endInclusive = end.nonEmpty && !isLastKeyIndex
    new PartitionRange(start, startInclusive, end, endInclusive, partition.index, dt, pred)
  }


  /**
   * Return the start keys of all of the regions in this table,
   * as a list of SparkImmutableBytesWritable.
   */
  def getRegionStartKeys = {
    val byteKeys: Array[HBaseRawType] = htable.getStartKeys
    val ret = ArrayBuffer[HBaseRawType]()

    // Since the size of byteKeys will be 1 if there is only one partition in the table,
    // we need to omit the that null element.
    if (!(byteKeys.length == 1 && byteKeys(0).length == 0)) {
      for (byteKey <- byteKeys) {
        ret += byteKey
      }
    }

    ret
  }

  def buildFilter(
                   projList: Seq[NamedExpression],
                   pred: Option[Expression]): (Option[FilterList], Option[Expression]) = {
    var distinctProjList = projList.distinct
    if (pred.isDefined) {
      distinctProjList = distinctProjList.filterNot(_.references.subsetOf(pred.get.references))
    }
    val projFilterList = if (distinctProjList.size == allColumns.size) {
      None
    } else {
      val filtersList: List[Filter] = nonKeyColumns.filter {
        case nkc => distinctProjList.exists(nkc.sqlName == _.name)
      }.map {
        case nkc@NonKeyColumn(_, _, family, qualifier) =>
          val columnFilters = new java.util.ArrayList[Filter]
          columnFilters.add(
            new FamilyFilter(
              CompareFilter.CompareOp.EQUAL,
              new BinaryComparator(nkc.familyRaw)
            ))
          columnFilters.add(
            new QualifierFilter(
              CompareFilter.CompareOp.EQUAL,
              new BinaryComparator(nkc.qualifierRaw)
            ))
          new FilterList(FilterList.Operator.MUST_PASS_ALL, columnFilters)
      }.toList

      Option(new FilterList(FilterList.Operator.MUST_PASS_ONE, filtersList.asJava))
    }

    if (pred.isDefined) {
      val predExp: Expression = pred.get
      // build pred pushdown filters:
      // 1. push any NOT through AND/OR
      val notPushedPred = NotPusher(predExp)
      // 2. classify the transformed predicate into pushdownable and non-pushdownable predicates
      val classier = new ScanPredClassifier(this, 0) // Right now only on primary key dimension
      val (pushdownFilterPred, otherPred) = classier(notPushedPred)
      // 3. build a FilterList mirroring the pushdownable predicate
      val predPushdownFilterList = buildFilterListFromPred(pushdownFilterPred)
      // 4. merge the above FilterList with the one from the projection
      (predPushdownFilterList, otherPred)
    } else {
      (projFilterList, None)
    }
  }

  private def buildFilterListFromPred(pred: Option[Expression]): Option[FilterList] = {
    var result: Option[FilterList] = None
    val filters = new java.util.ArrayList[Filter]
    if (pred.isDefined) {
      val expression = pred.get
      expression match {
        case And(left, right) =>
          if (left != null) {
            val leftFilterList = buildFilterListFromPred(Some(left))
            if (leftFilterList.isDefined) {
              val filterList = leftFilterList.get
              val size = filterList.getFilters.size
              if (size == 1 || filterList.getOperator == FilterList.Operator.MUST_PASS_ALL) {
                for (i <- 0 until size) {
                  filters.add(filterList.getFilters.get(i))
                }
              } else {
                filters.add(filterList)
              }
            }
          }
          if (right != null) {
            val rightFilterList = buildFilterListFromPred(Some(right))
            if (rightFilterList.isDefined) {
              val filterList = rightFilterList.get
              val size = filterList.getFilters.size
              if (size == 1 || filterList.getOperator == FilterList.Operator.MUST_PASS_ALL) {
                for (i <- 0 until size) {
                  filters.add(filterList.getFilters.get(i))
                }
              } else {
                filters.add(filterList)
              }
            }
          }
          result = Option(new FilterList(FilterList.Operator.MUST_PASS_ALL, filters))
        case Or(left, right) =>
          if (left != null) {
            val leftFilterList = buildFilterListFromPred(Some(left))
            if (leftFilterList.isDefined) {
              val filterList = leftFilterList.get
              val size = filterList.getFilters.size
              if (size == 1 || filterList.getOperator == FilterList.Operator.MUST_PASS_ONE) {
                for (i <- 0 until size) {
                  filters.add(filterList.getFilters.get(i))
                }
              } else {
                filters.add(filterList)
              }
            }
          }
          if (right != null) {
            val rightFilterList = buildFilterListFromPred(Some(right))
            if (rightFilterList.isDefined) {
              val filterList = rightFilterList.get
              val size = filterList.getFilters.size
              if (size == 1 || filterList.getOperator == FilterList.Operator.MUST_PASS_ONE) {
                for (i <- 0 until size) {
                  filters.add(filterList.getFilters.get(i))
                }
              } else {
                filters.add(filterList)
              }
            }
          }
          result = Option(new FilterList(FilterList.Operator.MUST_PASS_ONE, filters))
        case GreaterThan(left: AttributeReference, right: Literal) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(left.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(left.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.GREATER,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(right.value, right.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.GREATER,
              DataTypeUtils.getComparator(BytesUtils.create(right.dataType), right))
            result = Option(new FilterList(filter))
          }
        case GreaterThan(left: Literal, right: AttributeReference) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(right.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(right.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.GREATER,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(left.value, left.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.GREATER,
              DataTypeUtils.getComparator(BytesUtils.create(left.dataType), left))
            result = Option(new FilterList(filter))
          }
        case GreaterThanOrEqual(left: AttributeReference, right: Literal) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(left.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(left.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(right.value, right.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              DataTypeUtils.getComparator(BytesUtils.create(right.dataType), right))
            result = Option(new FilterList(filter))
          }
        case GreaterThanOrEqual(left: Literal, right: AttributeReference) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(right.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(right.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(left.value, left.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              DataTypeUtils.getComparator(BytesUtils.create(left.dataType), left))
            result = Option(new FilterList(filter))
          }
        case EqualTo(left: AttributeReference, right: Literal) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(left.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(left.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(right.value, right.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.EQUAL,
              DataTypeUtils.getComparator(BytesUtils.create(right.dataType), right))
            result = Option(new FilterList(filter))
          }
        case EqualTo(left: Literal, right: AttributeReference) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(right.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(right.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(left.value, left.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.EQUAL,
              DataTypeUtils.getComparator(BytesUtils.create(left.dataType), left))
            result = Option(new FilterList(filter))
          }
        case LessThan(left: AttributeReference, right: Literal) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(left.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(left.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.LESS,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(right.value, right.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.LESS,
              DataTypeUtils.getComparator(BytesUtils.create(right.dataType), right))
            result = Option(new FilterList(filter))
          }
        case LessThan(left: Literal, right: AttributeReference) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(right.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(right.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.LESS,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(left.value, left.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.LESS,
              DataTypeUtils.getComparator(BytesUtils.create(left.dataType), left))
            result = Option(new FilterList(filter))
          }
        case LessThanOrEqual(left: AttributeReference, right: Literal) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(left.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(left.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(right.value, right.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              DataTypeUtils.getComparator(BytesUtils.create(right.dataType), right))
            result = Option(new FilterList(filter))
          }
        case LessThanOrEqual(left: Literal, right: AttributeReference) =>
          val keyColumn = keyColumns.find((p: KeyColumn) => p.sqlName.equals(right.name))
          val nonKeyColumn = nonKeyColumns.find((p: NonKeyColumn) => p.sqlName.equals(right.name))
          if (keyColumn.isDefined) {
            val filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
              new BinaryPrefixComparator(DataTypeUtils.dataToBytes(left.value, left.dataType)))
            result = Option(new FilterList(filter))
          } else if (nonKeyColumn.isDefined) {
            val column = nonKeyColumn.get
            val filter = new SingleColumnValueFilter(column.familyRaw,
              column.qualifierRaw,
              CompareFilter.CompareOp.LESS_OR_EQUAL,
              DataTypeUtils.getComparator(BytesUtils.create(left.dataType), left))
            result = Option(new FilterList(filter))
          }
      }
    }
    result
  }

  def buildPut(row: Row): Put = {
    // TODO: revisit this using new KeyComposer
    val rowKey: HBaseRawType = null
    new Put(rowKey)
  }

  def sqlContext = context

  def schema: StructType = StructType(allColumns.map {
    case KeyColumn(name, dt, _) => StructField(name, dt, nullable = false)
    case NonKeyColumn(name, dt, _, _) => StructField(name, dt, nullable = true)
  })

  override def insert(data: DataFrame, overwrite: Boolean) = {
    if (!overwrite) {
      sqlContext.sparkContext.runJob(data.rdd, writeToHBase _)
    } else {
      // TODO: Support INSERT OVERWITE INTO
      sys.error("HBASE Table doesnot support INSERT OVERWRITE for now.")
    }
  }
  
  def writeToHBase(context: TaskContext, iterator: Iterator[Row]) = {
    // TODO:make the BatchMaxSize configurable
    val BatchMaxSize = 100
    
    var rowIndexInBatch = 0
    var colIndexInBatch = 0

    var puts = new ListBuffer[Put]()
    while (iterator.hasNext) {
      val row = iterator.next()
      val rawKeyCol = keyColumns.map(
        kc => {
          val rowColumn = DataTypeUtils.getRowColumnInHBaseRawType(
            row, kc.ordinal, kc.dataType)
          colIndexInBatch += 1
          (rowColumn, kc.dataType)
        }
      )
      val key = HBaseKVHelper.encodingRawKeyColumns(rawKeyCol)
      val put = new Put(key)
      nonKeyColumns.foreach(
        nkc => {
          val rowVal = DataTypeUtils.getRowColumnInHBaseRawType(
            row, nkc.ordinal, nkc.dataType)
          colIndexInBatch += 1
          put.add(nkc.familyRaw, nkc.qualifierRaw, rowVal)
        }
      )

      puts += put
      colIndexInBatch = 0
      rowIndexInBatch += 1
      if (rowIndexInBatch >= BatchMaxSize) {
        htable.put(puts.toList)
        puts.clear()
        rowIndexInBatch = 0
      }
    }
    if (puts.nonEmpty) {
      htable.put(puts.toList)
    }
    closeHTable()
  }
  
  
  def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    require(filters.size < 2, "Internal logical error: unexpected filter list size")
    val filterPredicate = if (filters.isEmpty) None
    else Some(filters(0))
    new HBaseSQLReaderRDD(
      this,
      context.conf.codegenEnabled,
      requiredColumns,
      filterPredicate, // PartitionPred : Option[Expression]
      None, // coprocSubPlan: SparkPlan
      context
    )
  }

  def buildScan(
                 split: Partition,
                 filters: Option[FilterList],
                 projList: Seq[NamedExpression]): Scan = {
    val hbPartition = split.asInstanceOf[HBasePartition]
    val scan = {
      (hbPartition.start, hbPartition.end) match {
        case (Some(lb), Some(ub)) => new Scan(lb, ub)
        case (Some(lb), None) => new Scan(lb)
        case (None, Some(ub)) => new Scan(Array[Byte](), ub)
        case _ => new Scan
      }
    }
    if (filters.isDefined && !filters.get.getFilters.isEmpty) {
      scan.setFilter(filters.get)
    }
    // TODO: add Family to SCAN from projections
    scan
  }

  def buildGet(projList: Seq[NamedExpression], rowKey: HBaseRawType) {
    new Get(rowKey)
    // TODO: add columns to the Get
  }

  def buildRow(projections: Seq[(Attribute, Int)],
               result: Result,
               buffer: ListBuffer[HBaseRawType],
               aBuffer: ArrayBuffer[Byte],
               row: MutableRow): Row = {
    assert(projections.size == row.length, "Projection size and row size mismatched")
    // TODO: replaced with the new Key method
    val rowKeys = HBaseKVHelper.decodingRawKeyColumns(result.getRow, keyColumns)
    projections.foreach { p =>
      columnMap.get(p._1.name).get match {
        case column: NonKeyColumn =>
          val colValue = result.getValue(column.familyRaw, column.qualifierRaw)
          DataTypeUtils.setRowColumnFromHBaseRawType(
            row, p._2, colValue, 0, colValue.length, column.dataType)
        case ki =>
          val keyIndex = ki.asInstanceOf[Int]
          val (start, length) = rowKeys(keyIndex)
          DataTypeUtils.setRowColumnFromHBaseRawType(
            row, p._2, result.getRow, start, length, keyColumns(keyIndex).dataType)
      }
    }
    row
  }

  /**
   * Convert the row key to its proper format. Due to the nature of HBase, the start and
   * end of partition could be partial row key, we may need to append 0x00 to make it comply
   * with the definition of key columns, for example, add four 0x00 if a key column type is
   * integer. Also string type will always be even number, so we need to add one 0x00 or two
   * depends on the length of the byte.
   * @param rawKey the original row key
   * @return the proper row key based on the definition of the key columns
   */
  def getFinalKey(rawKey: Option[HBaseRawType]): HBaseRawType = {
    val origRowKey: HBaseRawType = rawKey.get

    /**
     * Recursively run this function to check the key columns one by one.
     * If the input raw key contains the whole part of this key columns, then continue to
     * check the next one; otherwise, append the raw key by adding 0x00 to its proper format
     * and return it.
     * @param rowIndex the start point of unchecked bytes in the input raw key
     * @param curKeyIndex the next key column need to be checked
     * @return the proper row key based on the definition of the key columns
     */
    def getFinalRowKey(rowIndex: Int, curKeyIndex: Int): HBaseRawType = {
      if (curKeyIndex >= keyColumns.length) origRowKey
      else {
        val typeOfKey = keyColumns(curKeyIndex)
        if (typeOfKey.dataType == StringType) {
          val indexOfStringEnd = origRowKey.indexOf(0x00, rowIndex)
          if (indexOfStringEnd == -1) {
            val delta: Array[Byte] = if ((origRowKey.length - rowIndex) % 2 == 0) {
              new Array[Byte](1 + getMinimum(curKeyIndex + 1))
            } else {
              new Array[Byte](2 + getMinimum(curKeyIndex + 1))
            }
            origRowKey ++ delta
          } else {
            getFinalRowKey(indexOfStringEnd + 1, curKeyIndex + 1)
          }
        } else {
          val nextRowIndex = rowIndex +
            typeOfKey.dataType.asInstanceOf[NativeType].defaultSize
          if (nextRowIndex <= origRowKey.length) {
            getFinalRowKey(nextRowIndex, curKeyIndex + 1)
          } else {
            val delta: Array[Byte] = {
              new Array[Byte](nextRowIndex - origRowKey.length + getMinimum(curKeyIndex + 1))
            }
            origRowKey ++ delta
          }
        }
      }
    }

    /**
     * Get the minimum key length based on the key columns definition
     * @param startKeyIndex the start point of the key column
     * @return the minimum length required for the remaining key columns
     */
    def getMinimum(startKeyIndex: Int): Int = {
      keyColumns.drop(startKeyIndex).map(k => {
        k.dataType match {
          case StringType => 1
          case _ => k.dataType.asInstanceOf[NativeType].defaultSize
        }
      }
      ).sum
    }

    getFinalRowKey(0, 0)
  }

  /**
   * Convert a HBase row key into column values in their native data formats
   * @param rawKey the HBase row key
   * @return A sequence of column values from the row Key
   */
  def nativeKeyConvert(rawKey: Option[HBaseRawType]): Seq[Any] = {
    if (rawKey.isEmpty) Nil
    else {
      val finalRowKey = getFinalKey(rawKey)

      HBaseKVHelper.decodingRawKeyColumns(finalRowKey, keyColumns).
        zipWithIndex.map(pi => DataTypeUtils.bytesToData(finalRowKey,
        pi._1._1, pi._1._2, keyColumns(pi._2).dataType))
    }
  }
}

