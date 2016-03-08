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

package org.apache.spark.sql.execution.command

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.catalyst.parser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType


/**
 * Helper object to parse alter table commands.
 */
object AlterTableCommandParser {
  import ParserUtils._

  /**
   * Parse the given node assuming it is an alter table command.
   */
  def parse(v1: ASTNode): LogicalPlan = {
    v1.children match {
      case (tabName @ Token("TOK_TABNAME", _)) :: restNodes =>
        val tableIdent: TableIdentifier = extractTableIdent(tabName)
        val partitionSpec = getClauseOption("TOK_PARTSPEC", v1.children)
        val partition = partitionSpec.flatMap(parsePartitionSpec)
        matchAlterTableCommands(v1, restNodes, tableIdent, partition)
      case _ =>
        throw new AnalysisException(s"Could not parse alter table command: '${v1.text}'")
    }
  }

  private def cleanAndUnquoteString(s: String): String = {
    cleanIdentifier(unquoteString(s))
  }

  private def parsePartitionSpec(node: ASTNode): Option[Map[String, Option[String]]] = {
    node match {
      case Token("TOK_PARTSPEC", partitions) =>
        val spec = partitions.map {
          case Token("TOK_PARTVAL", ident :: constant :: Nil) =>
            (cleanAndUnquoteString(ident.text), Some(cleanAndUnquoteString(constant.text)))
          case Token("TOK_PARTVAL", ident :: Nil) =>
            (cleanAndUnquoteString(ident.text), None)
        }.toMap
        Some(spec)
      case _ => None
    }
  }

  private def extractTableProps(node: ASTNode): Map[String, Option[String]] = {
    node match {
      case Token("TOK_TABLEPROPERTIES", propsList) =>
        propsList.flatMap {
          case Token("TOK_TABLEPROPLIST", props) =>
            props.map {
              case Token("TOK_TABLEPROPERTY", key :: Token("TOK_NULL", Nil) :: Nil) =>
                val k = cleanAndUnquoteString(key.text)
                (k, None)
              case Token("TOK_TABLEPROPERTY", key :: value :: Nil) =>
                val k = cleanAndUnquoteString(key.text)
                val v = cleanAndUnquoteString(value.text)
                (k, Some(v))
            }
        }.toMap
      case _ =>
        throw new AnalysisException(
          s"Expected table properties in alter table command: '${node.text}'")
    }
  }

  // TODO: This method is massive. Break it down. Also, add some comments...
  private def matchAlterTableCommands(
      node: ASTNode,
      nodes: Seq[ASTNode],
      tableIdent: TableIdentifier,
      partition: Option[Map[String, Option[String]]]): LogicalPlan = {
    nodes match {
      case rename @ Token("TOK_ALTERTABLE_RENAME", renameArgs) :: _ =>
        val renamedTable = getClause("TOK_TABNAME", renameArgs)
        val renamedTableIdent: TableIdentifier = extractTableIdent(renamedTable)
        AlterTableRename(tableIdent, renamedTableIdent)(node.source)

      case Token("TOK_ALTERTABLE_PROPERTIES", args) :: _ =>
        val setTableProperties = extractTableProps(args.head)
        AlterTableSetProperties(
          tableIdent,
          setTableProperties)(node.source)

      case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) :: _ =>
        val dropTableProperties = extractTableProps(args.head)
        val allowExisting = getClauseOption("TOK_IFEXISTS", args)
        AlterTableDropProperties(
          tableIdent,
          dropTableProperties, allowExisting.isDefined)(node.source)

      case Token("TOK_ALTERTABLE_SERIALIZER", Token(serdeClassName, Nil) :: serdeArgs) :: _ =>
        // When SET SERDE serde_classname WITH SERDEPROPERTIES, this is None
        val serdeProperties: Option[Map[String, Option[String]]] =
          serdeArgs.headOption.map(extractTableProps)
        AlterTableSerDeProperties(
          tableIdent,
          Some(cleanAndUnquoteString(serdeClassName)),
          serdeProperties,
          partition)(node.source)

      case Token("TOK_ALTERTABLE_SERDEPROPERTIES", args) :: _ =>
        val serdeProperties: Map[String, Option[String]] = extractTableProps(args.head)
        AlterTableSerDeProperties(
          tableIdent,
          None,
          Some(serdeProperties),
          partition)(node.source)

      case Token("TOK_ALTERTABLE_CLUSTER_SORT", clusterSortArgs :: Nil) :: _ =>
        clusterSortArgs match {
          case Token("TOK_ALTERTABLE_BUCKETS", bucketArgsHead :: bucketArgs) =>
            val bucketCols = bucketArgsHead.children.map(_.text)
            val (sortCols, sortDirections, numBuckets) = {
              if (bucketArgs.head.text == "TOK_TABCOLNAME") {
                val (cols, directions) = bucketArgs.head.children.map {
                  case Token("TOK_TABSORTCOLNAMEASC", Token(colName, Nil) :: Nil) =>
                    (colName, Ascending)
                  case Token("TOK_TABSORTCOLNAMEDESC", Token(colName, Nil) :: Nil) =>
                    (colName, Descending)
                }.unzip
                (cols, directions, bucketArgs.last.text.toInt)
              } else {
                (Nil, Nil, bucketArgs.head.text.toInt)
              }
            }
            AlterTableStoreProperties(
              tableIdent,
              Some(BucketSpec(numBuckets, bucketCols, sortCols, sortDirections)),
              clustered = true,
              sorted = true)(node.source)
          case Token("TOK_NOT_CLUSTERED", Nil) =>
            AlterTableStoreProperties(
              tableIdent, None, clustered = false, sorted = true)(node.source)
          case Token("TOK_NOT_SORTED", Nil) =>
            AlterTableStoreProperties(
              tableIdent, None, clustered = true, sorted = false)(node.source)
        }

      case Token("TOK_ALTERTABLE_BUCKETS", Token(bucketNum, Nil) :: Nil) :: _ =>
        val num = bucketNum.toInt
        val buckets = Some(BucketSpec(num, Nil, Nil, Nil))
        AlterTableStoreProperties(
          tableIdent,
          buckets,
          clustered = true,
          sorted = true)(node.source)

      case Token("TOK_ALTERTABLE_SKEWED", Nil) :: _ =>
        // ALTER TABLE table_name NOT SKEWED
        AlterTableSkewed(
          tableIdent,
          Nil,
          Nil,
          storedAsDirs = false,
          notSkewed = true,
          notStoredAsDirs = false)(node.source)

      case Token("TOK_ALTERTABLE_SKEWED", Token("TOK_STOREDASDIRS", Nil) :: Nil) =>
        // ALTER TABLE table_name NOT STORED AS DIRECTORIES
        AlterTableSkewed(
          tableIdent,
          Nil,
          Nil,
          storedAsDirs = false,
          notSkewed = false,
          notStoredAsDirs = true)(node.source)

      case (tableSkewed @ Token("TOK_ALTERTABLE_SKEWED", _)) :: _ =>
        val skewedArgs = tableSkewed match {
          case Token("TOK_ALTERTABLE_SKEWED", args :: Nil) =>
            args match {
              case Token("TOK_TABLESKEWED", skewedCols :: skewedValues :: stored) =>
                val cols = skewedCols.children.map(n => cleanAndUnquoteString(n.text))
                val values = skewedValues match {
                  case Token("TOK_TABCOLVALUE", colVal) =>
                    Seq(colVal.map(n => cleanAndUnquoteString(n.text)))
                  case Token("TOK_TABCOLVALUE_PAIR", pairs) =>
                    pairs.map {
                      case Token("TOK_TABCOLVALUES", colVals :: Nil) =>
                        colVals match {
                          case Token("TOK_TABCOLVALUE", vals) =>
                            vals.map(n => cleanAndUnquoteString(n.text))
                        }
                    }
                }

                val storedAsDirs = stored match {
                  case Token("TOK_STOREDASDIRS", Nil) :: Nil => true
                  case _ => false
                }

                (cols, values, storedAsDirs)
            }
        }
        val (cols, values, storedAsDirs) = skewedArgs
        AlterTableSkewed(
          tableIdent,
          cols,
          values,
          storedAsDirs,
          notSkewed = false,
          notStoredAsDirs = false)(node.source)

      case Token("TOK_ALTERTABLE_SKEWED_LOCATION",
      Token("TOK_SKEWED_LOCATIONS",
      Token("TOK_SKEWED_LOCATION_LIST", locationMaps) :: Nil) :: Nil) :: _ =>
        val skewedMaps = locationMaps.map {
          case Token("TOK_SKEWED_LOCATION_MAP", key :: value :: Nil) =>
            val k = key match {
              case Token(const, Nil) => Seq(cleanAndUnquoteString(const))
              case Token("TOK_TABCOLVALUES", values :: Nil) =>
                values match {
                  case Token("TOK_TABCOLVALUE", vals) =>
                    vals.map(n => cleanAndUnquoteString(n.text))
                }
            }
            (k, cleanAndUnquoteString(value.text))
        }.toMap
        AlterTableSkewedLocation(tableIdent, skewedMaps)(node.source)

      case Token("TOK_ALTERTABLE_ADDPARTS", addPartsArgs) :: _ =>
        val (allowExisting, parts) = addPartsArgs match {
          case Token("TOK_IFNOTEXISTS", Nil) :: others => (true, others)
          case _ => (false, addPartsArgs)
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
        AlterTableAddPartition(tableIdent, partitions, allowExisting)(node.source)

      case Token("TOK_ALTERTABLE_RENAMEPART", partArg :: Nil) :: _ =>
        val Some(newPartition) = parsePartitionSpec(partArg)
        AlterTableRenamePartition(tableIdent, partition.get, newPartition)(node.source)

      case Token("TOK_ALTERTABLE_EXCHANGEPARTITION",
      (p @ Token("TOK_PARTSPEC", _)) :: (t @ Token("TOK_TABNAME", _)) :: Nil) :: _ =>
        val Some(partition) = parsePartitionSpec(p)
        val fromTableIdent = extractTableIdent(t)
        AlterTableExchangePartition(tableIdent, fromTableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_DROPPARTS", args) :: _ =>
        val parts = args.collect {
          case Token("TOK_PARTSPEC", partitions) =>
            partitions.map {
              case Token("TOK_PARTVAL", ident :: op :: constant :: Nil) =>
                (cleanAndUnquoteString(ident.text),
                  op.text, cleanAndUnquoteString(constant.text))
            }
        }
        val allowExisting = getClauseOption("TOK_IFEXISTS", args).isDefined
        val purge = getClauseOption("PURGE", args)
        val replication = getClauseOption("TOK_REPLICATION", args).map {
          case Token("TOK_REPLICATION", replId :: metadata) =>
            (cleanAndUnquoteString(replId.text), metadata.nonEmpty)
        }
        AlterTableDropPartition(
          tableIdent,
          parts,
          allowExisting,
          purge.isDefined,
          replication)(node.source)

      case Token("TOK_ALTERTABLE_ARCHIVE", partArg :: Nil) :: _ =>
        val Some(partition) = parsePartitionSpec(partArg)
        AlterTableArchivePartition(tableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_UNARCHIVE", partArg :: Nil) :: _ =>
        val Some(partition) = parsePartitionSpec(partArg)
        AlterTableUnarchivePartition(tableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_FILEFORMAT", args) :: _ =>
        val Seq(fileFormat, genericFormat) =
          getClauses(Seq("TOK_TABLEFILEFORMAT", "TOK_FILEFORMAT_GENERIC"),
            args)
        val fFormat = fileFormat.map(_.children.map(n => cleanAndUnquoteString(n.text)))
        val gFormat = genericFormat.map(f => cleanAndUnquoteString(f.children(0).text))
        AlterTableSetFileFormat(tableIdent, partition, fFormat, gFormat)(node.source)

      case Token("TOK_ALTERTABLE_LOCATION", Token(loc, Nil) :: Nil) :: _ =>
        AlterTableSetLocation(tableIdent, partition, cleanAndUnquoteString(loc))(node.source)

      case Token("TOK_ALTERTABLE_TOUCH", args) :: _ =>
        val part = getClauseOption("TOK_PARTSPEC", args).flatMap(parsePartitionSpec)
        AlterTableTouch(tableIdent, part)(node.source)

      case Token("TOK_ALTERTABLE_COMPACT", Token(compactType, Nil) :: Nil) :: _ =>
        AlterTableCompact(tableIdent, partition, cleanAndUnquoteString(compactType))(node.source)

      case Token("TOK_ALTERTABLE_MERGEFILES", _) :: _ =>
        AlterTableMerge(tableIdent, partition)(node.source)

      case Token("TOK_ALTERTABLE_RENAMECOL", args) :: _ =>
        val oldName = args(0).text
        val newName = args(1).text
        val dataType = nodeToDataType(args(2))
        val afterPos =
          getClauseOption("TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION", args)
        val afterPosCol = afterPos.map { ap =>
          ap.children match {
            case Token(col, Nil) :: Nil => col
            case _ => null
          }
        }
        val restrict = getClauseOption("TOK_RESTRICT", args)
        val cascade = getClauseOption("TOK_CASCADE", args)
        val comment = if (args.size > 3) {
          args(3) match {
            case Token(commentStr, Nil)
              if commentStr != "TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION" &&
                commentStr != "TOK_RESTRICT" && commentStr != "TOK_CASCADE" =>
              Some(cleanAndUnquoteString(commentStr))
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

      case Token("TOK_ALTERTABLE_ADDCOLS", args) :: _ =>
        val tableCols = getClause("TOK_TABCOLLIST", args)
        val columns = tableCols match {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }
        val restrict = getClauseOption("TOK_RESTRICT", args)
        val cascade = getClauseOption("TOK_CASCADE", args)
        AlterTableAddCol(
          tableIdent,
          partition,
          columns,
          restrict.isDefined,
          cascade.isDefined)(node.source)

      case Token("TOK_ALTERTABLE_REPLACECOLS", args) :: _ =>
        val tableCols = getClause("TOK_TABCOLLIST", args)
        val columns = tableCols match {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }
        val restrict = getClauseOption("TOK_RESTRICT", args)
        val cascade = getClauseOption("TOK_CASCADE", args)
        AlterTableReplaceCol(
          tableIdent,
          partition,
          columns,
          restrict.isDefined,
          cascade.isDefined)(node.source)

      case _ =>
        throw new AnalysisException(
          s"Unexpected children nodes in alter table command: '${node.text}'")
    }
  }

}
