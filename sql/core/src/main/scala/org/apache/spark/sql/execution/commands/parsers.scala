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

package org.apache.spark.sql.execution.commands

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{CatalystQl, PlanParser, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.catalyst.parser.{ASTNode, ParserConf, SimpleParserConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.commands._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

case class AlterTableCommandParser(base: CatalystQl) extends PlanParser {

  def parsePartitionSpec(node: ASTNode): Option[Map[String, Option[String]]] = {
    node match {
      case Token("TOK_PARTSPEC", partitions) =>
        val spec = partitions.map {
          case Token("TOK_PARTVAL", ident :: constant :: Nil) =>
            (unquoteString(cleanIdentifier(ident.text)),
              Some(unquoteString(cleanIdentifier(constant.text))))
          case Token("TOK_PARTVAL", ident :: Nil) =>
            (unquoteString(cleanIdentifier(ident.text)), None)
        }.toMap
        Some(spec)
      case _ => None
    }
  }

  def extractTableProps(node: ASTNode): Map[String, Option[String]] = node match {
    case Token("TOK_TABLEPROPERTIES", propsList) =>
      propsList.flatMap {
        case Token("TOK_TABLEPROPLIST", props) =>
          props.map {
            case Token("TOK_TABLEPROPERTY", key :: Token("TOK_NULL", Nil) :: Nil) =>
              val k = unquoteString(cleanIdentifier(key.text))
              (k, None)
            case Token("TOK_TABLEPROPERTY", key :: value :: Nil) =>
              val k = unquoteString(cleanIdentifier(key.text))
              val v = unquoteString(cleanIdentifier(value.text))
              (k, Some(v))
          }
      }.toMap
  }

  override def isDefinedAt(node: ASTNode): Boolean = node.text == "TOK_ALTERTABLE"

  override def apply(v1: ASTNode): LogicalPlan = v1.children match {
    case (tabName @ Token("TOK_TABNAME", _)) :: rest =>
      val tableIdent: TableIdentifier = base.extractTableIdent(tabName)
      val partitionSpec = base.getClauseOption("TOK_PARTSPEC", v1.children)
      val partition = partitionSpec.flatMap(parsePartitionSpec)
      matchAlterTableCommands(v1, rest, tableIdent, partition)
    case _ =>
      throw new NotImplementedError(v1.text)
  }

  def matchAlterTableCommands(
      node: ASTNode,
      nodes: Seq[ASTNode],
      tableIdent: TableIdentifier,
      partition: Option[Map[String, Option[String]]]): LogicalPlan = nodes match {
    case rename @ Token("TOK_ALTERTABLE_RENAME", renameArgs) :: rest =>
      val renamedTable = base.getClause("TOK_TABNAME", renameArgs)
      val renamedTableIdent: TableIdentifier = base.extractTableIdent(renamedTable)
      AlterTableRename(tableIdent, renamedTableIdent)(node.source)

    case Token("TOK_ALTERTABLE_PROPERTIES", args) :: rest =>
      val setTableProperties = extractTableProps(args.head)
      AlterTableSetProperties(
        tableIdent,
        setTableProperties)(node.source)

    case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) :: rest =>
      val dropTableProperties = extractTableProps(args.head)
      val allowExisting = base.getClauseOption("TOK_IFEXISTS", args)
      AlterTableDropProperties(
        tableIdent,
        dropTableProperties, allowExisting.isDefined)(node.source)

    case Token("TOK_ALTERTABLE_SERIALIZER", serdeArgs) :: rest =>
      val serdeClassName = unquoteString(cleanIdentifier(serdeArgs.head.text))

      val serdeProperties: Option[Map[String, Option[String]]] = Option(
        // SET SERDE serde_classname WITH SERDEPROPERTIES
        if (serdeArgs.tail.isEmpty) {
          null
        } else {
          extractTableProps(serdeArgs.tail.head)
        }
      )

      AlterTableSerDeProperties(
        tableIdent,
        Some(serdeClassName),
        serdeProperties,
        partition)(node.source)

    case Token("TOK_ALTERTABLE_SERDEPROPERTIES", args) :: rest =>
      val serdeProperties: Map[String, Option[String]] = extractTableProps(args.head)

      AlterTableSerDeProperties(
        tableIdent,
        None,
        Some(serdeProperties),
        partition)(node.source)

    case (bucketSpec @ Token("TOK_ALTERTABLE_CLUSTER_SORT", _)) :: rest =>
      val (buckets, noClustered, noSorted) = bucketSpec match {
        case Token("TOK_ALTERTABLE_CLUSTER_SORT", clusterAndSoryByArgs :: Nil) =>
          clusterAndSoryByArgs match {
              case Token("TOK_ALTERTABLE_BUCKETS", bucketArgs) =>
                val bucketCols = bucketArgs.head.children.map(_.text)

                val (sortCols, sortDirections, numBuckets) = {
                  if (bucketArgs(1).text == "TOK_TABCOLNAME") {
                    val cols = bucketArgs(1).children.map {
                      case Token("TOK_TABSORTCOLNAMEASC", Token(colName, Nil) :: Nil) =>
                        (colName, Ascending)
                      case Token("TOK_TABSORTCOLNAMEDESC", Token(colName, Nil) :: Nil) =>
                        (colName, Descending)
                    }
                    (cols.map(_._1), cols.map(_._2), bucketArgs(2).text.toInt)
                  } else {
                    (Nil, Nil, bucketArgs(1).text.toInt)
                  }
                }

                (Some(BucketSpec(numBuckets, bucketCols, sortCols, sortDirections)),
                  false, false)
              case Token("TOK_NOT_CLUSTERED", Nil) =>
                (None, true, false)
              case Token("TOK_NOT_SORTED", Nil) =>
                (None, false, true)
          }
      }

      AlterTableStoreProperties(
        tableIdent,
        buckets,
        noClustered,
        noSorted)(node.source)

    case Token("TOK_ALTERTABLE_BUCKETS", Token(bucketNum, Nil) :: Nil) :: rest =>
      val num = bucketNum.toInt
      val buckets = Some(BucketSpec(num, Nil, Nil, Nil))
      AlterTableStoreProperties(
        tableIdent,
        buckets,
        false,
        false)(node.source)

    case (tableSkewed @ Token("TOK_ALTERTABLE_SKEWED", _)) :: rest =>
      // Alter Table not skewed
      // Token("TOK_ALTERTABLE_SKEWED", Nil) means not skewed.
      val notSkewed = if (tableSkewed.children.size == 0) {
        true
      } else {
        false
      }

      val (notStoredAsDirs, skewedArgs) = tableSkewed match {
        case Token("TOK_ALTERTABLE_SKEWED", Token("TOK_STOREDASDIRS", Nil) :: Nil) =>
          // Alter Table not stored as directories
          (true, None)
        case Token("TOK_ALTERTABLE_SKEWED", skewedArgs :: Nil) =>
          val (cols, values, storedAsDirs) = skewedArgs match {
            case Token("TOK_TABLESKEWED", skewedCols :: skewedValues :: stored) =>
              val cols = skewedCols.children.map(n => unquoteString(cleanIdentifier(n.text)))
              val values = skewedValues match {
                case Token("TOK_TABCOLVALUE", values) =>
                  Seq(values.map(n => unquoteString(cleanIdentifier(n.text))))
                case Token("TOK_TABCOLVALUE_PAIR", pairs) =>
                  pairs.map {
                    case Token("TOK_TABCOLVALUES", values :: Nil) =>
                      values match {
                        case Token("TOK_TABCOLVALUE", vals) =>
                          vals.map(n => unquoteString(cleanIdentifier(n.text)))
                      }
                  }
              }

              val storedAsDirs = stored match {
                case Token("TOK_STOREDASDIRS", Nil) :: Nil => true
                case _ => false
              }

              (cols, values, storedAsDirs)
          }
          (false, Some((cols, values, storedAsDirs)))
      }

      if (skewedArgs.isDefined) {
        AlterTableSkewed(
          tableIdent,
          skewedArgs.get._1, /* cols */
          skewedArgs.get._2, /* values */
          skewedArgs.get._3, /* storedAsDirs */
          notSkewed, notStoredAsDirs)(node.source)
      } else {
        AlterTableSkewed(tableIdent, Nil, Nil, false, notSkewed, notStoredAsDirs)(node.source)
      }

    case Token("TOK_ALTERTABLE_SKEWED_LOCATION", args) :: rest =>
      val skewedMaps = args(0) match {
        case Token("TOK_SKEWED_LOCATIONS", locationList :: Nil) =>
          locationList match {
            case Token("TOK_SKEWED_LOCATION_LIST", locationMaps) =>
              locationMaps.map {
                case Token("TOK_SKEWED_LOCATION_MAP", key :: value :: Nil) =>
                  val k = key match {
                    case Token(const, Nil) => Seq(unquoteString(cleanIdentifier(const)))
                    case Token("TOK_TABCOLVALUES", values :: Nil) =>
                      values match {
                        case Token("TOK_TABCOLVALUE", vals) =>
                          vals.map(n => unquoteString(cleanIdentifier(n.text)))
                      }
                  }
                  (k, unquoteString(cleanIdentifier(value.text)))
              }.toMap
          }
      }
      AlterTableSkewedLocation(tableIdent, skewedMaps)(node.source)

    case Token("TOK_ALTERTABLE_ADDPARTS", addPartsArgs) :: rest =>
      val allowExisting = base.getClauseOption("TOK_IFNOTEXISTS", addPartsArgs)
      val parts = if (allowExisting.isDefined) {
        addPartsArgs.tail
      } else {
        addPartsArgs
      }

      val partitions: ArrayBuffer[(Map[String, Option[String]], Option[String])] =
        new ArrayBuffer()
      var currentPart: Map[String, Option[String]] = null
      parts.map {
        case t @ Token("TOK_PARTSPEC", partArgs) =>
          if (currentPart != null) {
            partitions += ((currentPart, None))
          }
          currentPart = parsePartitionSpec(t).get
        case Token("TOK_PARTITIONLOCATION", loc :: Nil) =>
          val location = unquoteString(loc.text)
          if (currentPart != null) {
            partitions += ((currentPart, Some(location)))
            currentPart = null
          } else {
            // We should not reach here
            throw new AnalysisException("Partition location must follow a partition spec.")
          }
      }

      if (currentPart != null) {
        partitions += ((currentPart, None))
      }
      AlterTableAddPartition(tableIdent, partitions, allowExisting.isDefined)(node.source)

    case Token("TOK_ALTERTABLE_RENAMEPART", args) :: rest =>
      val newPartition = parsePartitionSpec(args(0))
      AlterTableRenamePartition(tableIdent, partition.get, newPartition.get)(node.source)

    case Token("TOK_ALTERTABLE_EXCHANGEPARTITION", args) :: rest =>
      val Seq(Some(partSpec), Some(fromTable)) =
        base.getClauses(Seq("TOK_PARTSPEC", "TOK_TABNAME"), args)
      val partition = parsePartitionSpec(partSpec).get
      val fromTableIdent = base.extractTableIdent(fromTable)
      AlterTableExchangePartition(tableIdent, fromTableIdent, partition)(node.source)

    case Token("TOK_ALTERTABLE_DROPPARTS", args) :: rest =>
      val parts = args.collect {
        case Token("TOK_PARTSPEC", partitions) =>
          partitions.map {
            case Token("TOK_PARTVAL", ident :: op :: constant :: Nil) =>
              (unquoteString(cleanIdentifier(ident.text)),
                op.text, unquoteString(cleanIdentifier(constant.text)))
          }
      }

      val allowExisting = base.getClauseOption("TOK_IFEXISTS", args)
      val purge = base.getClauseOption("PURGE", args)

      val replication = base.getClauseOption("TOK_REPLICATION", args).map {
        case Token("TOK_REPLICATION", replId :: metadata :: Nil) =>
          (unquoteString(cleanIdentifier(replId.text)), true)
        case Token("TOK_REPLICATION", replId :: Nil) =>
          (unquoteString(cleanIdentifier(replId.text)), false)
      }

      AlterTableDropPartition(
        tableIdent,
        parts,
        allowExisting.isDefined,
        purge.isDefined,
        replication)(node.source)

    case Token("TOK_ALTERTABLE_ARCHIVE", args) :: rest =>
      val partition = parsePartitionSpec(args(0)).get
      AlterTableArchivePartition(tableIdent, partition)(node.source)

    case Token("TOK_ALTERTABLE_UNARCHIVE", args) :: rest =>
      val partition = parsePartitionSpec(args(0)).get
      AlterTableUnarchivePartition(tableIdent, partition)(node.source)

    case Token("TOK_ALTERTABLE_FILEFORMAT", args) :: rest =>
      val Seq(fileFormat, genericFormat) =
        base.getClauses(Seq("TOK_TABLEFILEFORMAT", "TOK_FILEFORMAT_GENERIC"),
          args)
      val fFormat = fileFormat.map(_.children.map(n => unquoteString(cleanIdentifier(n.text))))
      val gFormat = genericFormat.map(f => unquoteString(cleanIdentifier(f.children(0).text)))
      AlterTableSetFileFormat(tableIdent, partition, fFormat, gFormat)(node.source)

    case Token("TOK_ALTERTABLE_LOCATION", Token(loc, Nil) :: Nil) :: rest =>
      AlterTableSetLocation(tableIdent, partition, unquoteString(cleanIdentifier(loc)))(node.source)

    case Token("TOK_ALTERTABLE_TOUCH", args) :: rest =>
      val part = base.getClauseOption("TOK_PARTSPEC", args).flatMap(parsePartitionSpec)
      AlterTableTouch(tableIdent, part)(node.source)

    case Token("TOK_ALTERTABLE_COMPACT", Token(compactType, Nil) :: Nil) :: rest =>
      AlterTableCompact(tableIdent, partition,
        unquoteString(cleanIdentifier(compactType)))(node.source)

    case Token("TOK_ALTERTABLE_MERGEFILES", _) :: rest =>
      AlterTableMerge(tableIdent, partition)(node.source)

    case Token("TOK_ALTERTABLE_RENAMECOL", args) :: rest =>
      val oldName = args(0).text
      val newName = args(1).text
      val dataType = base.nodeToDataType(args(2))
      val afterPos =
        base.getClauseOption("TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION", args)
      val afterPosCol = afterPos.map { ap =>
        ap.children match {
          case Token(col, Nil) :: Nil => col
          case _ => null
        }
      }

      val restrict = base.getClauseOption("TOK_RESTRICT", args)
      val cascade = base.getClauseOption("TOK_CASCADE", args)

      val comment = if (args.size > 3) {
        args(3) match {
          case Token(commentStr, Nil)
            if commentStr != "TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION" &&
              commentStr != "TOK_RESTRICT" && commentStr != "TOK_CASCADE" =>
              Some(unquoteString(cleanIdentifier(commentStr)))
          case _ =>
            None
        }
      } else {
        None
      }

      AlterTableChangeCol(
        tableIdent,
        partition,
        oldName,
        newName,
        dataType,
        comment,
        afterPos.isDefined,
        afterPosCol,
        restrict.isDefined,
        cascade.isDefined)(node.source)

    case Token("TOK_ALTERTABLE_ADDCOLS", args) :: rest =>
      val tableCols = base.getClause("TOK_TABCOLLIST", args)
      val columns = tableCols match {
        case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(base.nodeToStructField))
      }

      val restrict = base.getClauseOption("TOK_RESTRICT", args)
      val cascade = base.getClauseOption("TOK_CASCADE", args)

      AlterTableAddCol(
        tableIdent,
        partition,
        columns,
        restrict.isDefined,
        cascade.isDefined)(node.source)

    case Token("TOK_ALTERTABLE_REPLACECOLS", args) :: rest =>
      val tableCols = base.getClause("TOK_TABCOLLIST", args)
      val columns = tableCols match {
        case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(base.nodeToStructField))
      }

      val restrict = base.getClauseOption("TOK_RESTRICT", args)
      val cascade = base.getClauseOption("TOK_CASCADE", args)

      AlterTableReplaceCol(
        tableIdent,
        partition,
        columns,
        restrict.isDefined,
        cascade.isDefined)(node.source)
  }
}

