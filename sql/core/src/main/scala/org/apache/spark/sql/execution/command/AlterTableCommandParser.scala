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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, SortDirection}
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
  def parse(node: ASTNode): LogicalPlan = {
    node.children match {
      case (tabName @ Token("TOK_TABNAME", _)) :: otherNodes =>
        val tableIdent = extractTableIdent(tabName)
        val partSpec = getClauseOption("TOK_PARTSPEC", node.children).map(parsePartitionSpec)
        matchAlterTableCommands(node, otherNodes, tableIdent, partSpec)
      case _ =>
        parseFailed("Could not parse ALTER TABLE command", node)
    }
  }

  private def cleanAndUnquoteString(s: String): String = {
    cleanIdentifier(unquoteString(s))
  }

  /**
   * Extract partition spec from the given [[ASTNode]] as a map, assuming it exists.
   *
   * Example format:
   *
   *   TOK_PARTSPEC
   *   :- TOK_PARTVAL
   *   :  :- dt
   *   :  +- '2008-08-08'
   *   +- TOK_PARTVAL
   *      :- country
   *      +- 'us'
   */
  private def parsePartitionSpec(node: ASTNode): Map[String, String] = {
    node match {
      case Token("TOK_PARTSPEC", partitions) =>
        partitions.map {
          // Note: sometimes there's a "=", "<" or ">" between the key and the value
          // (e.g. when dropping all partitions with value > than a certain constant)
          case Token("TOK_PARTVAL", ident :: conj :: constant :: Nil) =>
            (cleanAndUnquoteString(ident.text), cleanAndUnquoteString(constant.text))
          case Token("TOK_PARTVAL", ident :: constant :: Nil) =>
            (cleanAndUnquoteString(ident.text), cleanAndUnquoteString(constant.text))
          case Token("TOK_PARTVAL", ident :: Nil) =>
            (cleanAndUnquoteString(ident.text), null)
          case _ =>
            parseFailed("Invalid ALTER TABLE command", node)
        }.toMap
      case _ =>
        parseFailed("Expected partition spec in ALTER TABLE command", node)
    }
  }

  /**
   * Extract table properties from the given [[ASTNode]] as a map, assuming it exists.
   *
   * Example format:
   *
   *   TOK_TABLEPROPERTIES
   *   +- TOK_TABLEPROPLIST
   *      :- TOK_TABLEPROPERTY
   *      :  :- 'test'
   *      :  +- 'value'
   *      +- TOK_TABLEPROPERTY
   *         :- 'comment'
   *         +- 'new_comment'
   */
  private def extractTableProps(node: ASTNode): Map[String, String] = {
    node match {
      case Token("TOK_TABLEPROPERTIES", propsList) =>
        propsList.flatMap {
          case Token("TOK_TABLEPROPLIST", props) =>
            props.map { case Token("TOK_TABLEPROPERTY", key :: value :: Nil) =>
              val k = cleanAndUnquoteString(key.text)
              val v = value match {
                case Token("TOK_NULL", Nil) => null
                case _ => cleanAndUnquoteString(value.text)
              }
              (k, v)
            }
          case _ =>
            parseFailed("Invalid ALTER TABLE command", node)
        }.toMap
      case _ =>
        parseFailed("Expected table properties in ALTER TABLE command", node)
    }
  }

  /**
   * Parse an alter table command from a [[ASTNode]] into a [[LogicalPlan]].
   * This follows https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL.
   *
   * @param node the original [[ASTNode]] to parse.
   * @param otherNodes the other [[ASTNode]]s after the first one containing the table name.
   * @param tableIdent identifier of the table, parsed from the first [[ASTNode]].
   * @param partition spec identifying the partition this command is concerned with, if any.
   */
  // TODO: This method is massive. Break it down.
  private def matchAlterTableCommands(
      node: ASTNode,
      otherNodes: Seq[ASTNode],
      tableIdent: TableIdentifier,
      partition: Option[TablePartitionSpec]): LogicalPlan = {
    otherNodes match {
      // ALTER TABLE table_name RENAME TO new_table_name;
      case Token("TOK_ALTERTABLE_RENAME", renameArgs) :: _ =>
        val tableNameClause = getClause("TOK_TABNAME", renameArgs)
        val newTableIdent = extractTableIdent(tableNameClause)
        AlterTableRename(tableIdent, newTableIdent)(node.source)

      // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
      case Token("TOK_ALTERTABLE_PROPERTIES", args) :: _ =>
        val properties = extractTableProps(args.head)
        AlterTableSetProperties(tableIdent, properties)(node.source)

      // ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'key');
      case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) :: _ =>
        val properties = extractTableProps(args.head)
        val ifExists = getClauseOption("TOK_IFEXISTS", args).isDefined
        AlterTableUnsetProperties(tableIdent, properties, ifExists)(node.source)

      // ALTER TABLE table_name [PARTITION spec] SET SERDE serde_name [WITH SERDEPROPERTIES props];
      case Token("TOK_ALTERTABLE_SERIALIZER", Token(serdeClassName, Nil) :: serdeArgs) :: _ =>
        AlterTableSerDeProperties(
          tableIdent,
          Some(cleanAndUnquoteString(serdeClassName)),
          serdeArgs.headOption.map(extractTableProps),
          partition)(node.source)

      // ALTER TABLE table_name [PARTITION spec] SET SERDEPROPERTIES serde_properties;
      case Token("TOK_ALTERTABLE_SERDEPROPERTIES", args) :: _ =>
        AlterTableSerDeProperties(
          tableIdent,
          None,
          Some(extractTableProps(args.head)),
          partition)(node.source)

      // ALTER TABLE table_name CLUSTERED BY (col, ...) [SORTED BY (col, ...)] INTO n BUCKETS;
      case Token("TOK_ALTERTABLE_CLUSTER_SORT", Token("TOK_ALTERTABLE_BUCKETS", b) :: Nil) :: _ =>
        val clusterCols: Seq[String] = b.head match {
          case Token("TOK_TABCOLNAME", children) => children.map(_.text)
          case _ => parseFailed("Invalid ALTER TABLE command", node)
        }
        // If sort columns are specified, num buckets should be the third arg.
        // If sort columns are not specified, num buckets should be the second arg.
        // TODO: actually use `sortDirections` once we actually store that in the metastore
        val (sortCols: Seq[String], sortDirections: Seq[SortDirection], numBuckets: Int) = {
          b.tail match {
            case Token("TOK_TABCOLNAME", children) :: numBucketsNode :: Nil =>
              val (cols, directions) = children.map {
                case Token("TOK_TABSORTCOLNAMEASC", Token(col, Nil) :: Nil) => (col, Ascending)
                case Token("TOK_TABSORTCOLNAMEDESC", Token(col, Nil) :: Nil) => (col, Descending)
              }.unzip
              (cols, directions, numBucketsNode.text.toInt)
            case numBucketsNode :: Nil =>
              (Nil, Nil, numBucketsNode.text.toInt)
            case _ =>
              parseFailed("Invalid ALTER TABLE command", node)
          }
        }
        AlterTableStorageProperties(
          tableIdent,
          BucketSpec(numBuckets, clusterCols, sortCols))(node.source)

      // ALTER TABLE table_name NOT CLUSTERED
      case Token("TOK_ALTERTABLE_CLUSTER_SORT", Token("TOK_NOT_CLUSTERED", Nil) :: Nil) :: _ =>
        AlterTableNotClustered(tableIdent)(node.source)

      // ALTER TABLE table_name NOT SORTED
      case Token("TOK_ALTERTABLE_CLUSTER_SORT", Token("TOK_NOT_SORTED", Nil) :: Nil) :: _ =>
        AlterTableNotSorted(tableIdent)(node.source)

      // ALTER TABLE table_name SKEWED BY (col1, col2)
      //   ON ((col1_value, col2_value) [, (col1_value, col2_value), ...])
      //   [STORED AS DIRECTORIES];
      case Token("TOK_ALTERTABLE_SKEWED",
          Token("TOK_TABLESKEWED",
          Token("TOK_TABCOLNAME", colNames) :: colValues :: rest) :: Nil) :: _ =>
        // Example format:
        //
        //   TOK_ALTERTABLE_SKEWED
        //   :- TOK_TABLESKEWED
        //   :  :- TOK_TABCOLNAME
        //   :  :  :- dt
        //   :  :  +- country
        //   :- TOK_TABCOLVALUE_PAIR
        //   :  :- TOK_TABCOLVALUES
        //   :  :  :- TOK_TABCOLVALUE
        //   :  :  :  :- '2008-08-08'
        //   :  :  :  +- 'us'
        //   :  :- TOK_TABCOLVALUES
        //   :  :  :- TOK_TABCOLVALUE
        //   :  :  :  :- '2009-09-09'
        //   :  :  :  +- 'uk'
        //   +- TOK_STOREASDIR
        val names = colNames.map { n => cleanAndUnquoteString(n.text) }
        val values = colValues match {
          case Token("TOK_TABCOLVALUE", vals) =>
            Seq(vals.map { n => cleanAndUnquoteString(n.text) })
          case Token("TOK_TABCOLVALUE_PAIR", pairs) =>
            pairs.map {
              case Token("TOK_TABCOLVALUES", Token("TOK_TABCOLVALUE", vals) :: Nil) =>
                vals.map { n => cleanAndUnquoteString(n.text) }
              case _ =>
                parseFailed("Invalid ALTER TABLE command", node)
            }
          case _ =>
            parseFailed("Invalid ALTER TABLE command", node)
        }
        val storedAsDirs = rest match {
          case Token("TOK_STOREDASDIRS", Nil) :: Nil => true
          case _ => false
        }
        AlterTableSkewed(
          tableIdent,
          names,
          values,
          storedAsDirs)(node.source)

      // ALTER TABLE table_name NOT SKEWED
      case Token("TOK_ALTERTABLE_SKEWED", Nil) :: _ =>
        AlterTableNotSkewed(tableIdent)(node.source)

      // ALTER TABLE table_name NOT STORED AS DIRECTORIES
      case Token("TOK_ALTERTABLE_SKEWED", Token("TOK_STOREDASDIRS", Nil) :: Nil) :: _ =>
        AlterTableNotStoredAsDirs(tableIdent)(node.source)

      // ALTER TABLE table_name SET SKEWED LOCATION (col1="loc1" [, (col2, col3)="loc2", ...] );
      case Token("TOK_ALTERTABLE_SKEWED_LOCATION",
        Token("TOK_SKEWED_LOCATIONS",
        Token("TOK_SKEWED_LOCATION_LIST", locationMaps) :: Nil) :: Nil) :: _ =>
        // Example format:
        //
        //   TOK_ALTERTABLE_SKEWED_LOCATION
        //   +- TOK_SKEWED_LOCATIONS
        //      +- TOK_SKEWED_LOCATION_LIST
        //         :- TOK_SKEWED_LOCATION_MAP
        //         :  :- 'col1'
        //         :  +- 'loc1'
        //         +- TOK_SKEWED_LOCATION_MAP
        //            :- TOK_TABCOLVALUES
        //            :  +- TOK_TABCOLVALUE
        //            :     :- 'col2'
        //            :     +- 'col3'
        //            +- 'loc2'
        val skewedMaps = locationMaps.flatMap {
          case Token("TOK_SKEWED_LOCATION_MAP", col :: loc :: Nil) =>
            col match {
              case Token(const, Nil) =>
                Seq((cleanAndUnquoteString(const), cleanAndUnquoteString(loc.text)))
              case Token("TOK_TABCOLVALUES", Token("TOK_TABCOLVALUE", keys) :: Nil) =>
                keys.map { k => (cleanAndUnquoteString(k.text), cleanAndUnquoteString(loc.text)) }
            }
          case _ =>
            parseFailed("Invalid ALTER TABLE command", node)
        }.toMap
        AlterTableSkewedLocation(tableIdent, skewedMaps)(node.source)

      // ALTER TABLE table_name ADD [IF NOT EXISTS] PARTITION spec [LOCATION 'loc1']
      // spec [LOCATION 'loc2'] ...;
      case Token("TOK_ALTERTABLE_ADDPARTS", args) :: _ =>
        val (ifNotExists, parts) = args.head match {
          case Token("TOK_IFNOTEXISTS", Nil) => (true, args.tail)
          case _ => (false, args)
        }
        // List of (spec, location) to describe partitions to add
        // Each partition spec may or may not be followed by a location
        val parsedParts = new ArrayBuffer[(TablePartitionSpec, Option[String])]
        parts.foreach {
          case t @ Token("TOK_PARTSPEC", _) =>
            parsedParts += ((parsePartitionSpec(t), None))
          case Token("TOK_PARTITIONLOCATION", loc :: Nil) =>
            // Update the location of the last partition we just added
            if (parsedParts.nonEmpty) {
              val (spec, _) = parsedParts.remove(parsedParts.length - 1)
              parsedParts += ((spec, Some(unquoteString(loc.text))))
            }
          case _ =>
            parseFailed("Invalid ALTER TABLE command", node)
        }
        AlterTableAddPartition(tableIdent, parsedParts, ifNotExists)(node.source)

      // ALTER TABLE table_name PARTITION spec1 RENAME TO PARTITION spec2;
      case Token("TOK_ALTERTABLE_RENAMEPART", spec :: Nil) :: _ =>
        val newPartition = parsePartitionSpec(spec)
        val oldPartition = partition.getOrElse {
          parseFailed("Expected old partition spec in ALTER TABLE rename partition command", node)
        }
        AlterTableRenamePartition(tableIdent, oldPartition, newPartition)(node.source)

      // ALTER TABLE table_name_1 EXCHANGE PARTITION spec WITH TABLE table_name_2;
      case Token("TOK_ALTERTABLE_EXCHANGEPARTITION", spec :: newTable :: Nil) :: _ =>
        val parsedSpec = parsePartitionSpec(spec)
        val newTableIdent = extractTableIdent(newTable)
        AlterTableExchangePartition(tableIdent, newTableIdent, parsedSpec)(node.source)

      // ALTER TABLE table_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
      case Token("TOK_ALTERTABLE_DROPPARTS", args) :: _ =>
        val parts = args.collect { case p @ Token("TOK_PARTSPEC", _) => parsePartitionSpec(p) }
        val ifExists = getClauseOption("TOK_IFEXISTS", args).isDefined
        val purge = getClauseOption("PURGE", args).isDefined
        AlterTableDropPartition(tableIdent, parts, ifExists, purge)(node.source)

      // ALTER TABLE table_name ARCHIVE PARTITION spec;
      case Token("TOK_ALTERTABLE_ARCHIVE", spec :: Nil) :: _ =>
        AlterTableArchivePartition(tableIdent, parsePartitionSpec(spec))(node.source)

      // ALTER TABLE table_name UNARCHIVE PARTITION spec;
      case Token("TOK_ALTERTABLE_UNARCHIVE", spec :: Nil) :: _ =>
        AlterTableUnarchivePartition(tableIdent, parsePartitionSpec(spec))(node.source)

      // ALTER TABLE table_name [PARTITION spec] SET FILEFORMAT file_format;
      case Token("TOK_ALTERTABLE_FILEFORMAT", args) :: _ =>
        val Seq(fileFormat, genericFormat) =
          getClauses(Seq("TOK_TABLEFILEFORMAT", "TOK_FILEFORMAT_GENERIC"), args)
        // Note: the AST doesn't contain information about which file format is being set here.
        // E.g. we can't differentiate between INPUTFORMAT and OUTPUTFORMAT if either is set.
        // Right now this just stores the values, but we should figure out how to get the keys.
        val fFormat = fileFormat
          .map { _.children.map { n => cleanAndUnquoteString(n.text) }}
          .getOrElse(Seq())
        val gFormat = genericFormat.map { f => cleanAndUnquoteString(f.children(0).text) }
        AlterTableSetFileFormat(tableIdent, partition, fFormat, gFormat)(node.source)

      // ALTER TABLE table_name [PARTITION spec] SET LOCATION "loc";
      case Token("TOK_ALTERTABLE_LOCATION", Token(loc, Nil) :: Nil) :: _ =>
        AlterTableSetLocation(tableIdent, partition, cleanAndUnquoteString(loc))(node.source)

      // ALTER TABLE table_name TOUCH [PARTITION spec];
      case Token("TOK_ALTERTABLE_TOUCH", args) :: _ =>
        // Note: the partition spec, if it exists, comes after TOUCH, so `partition` should
        // always be None here. Instead, we need to parse it from the TOUCH node's children.
        val part = getClauseOption("TOK_PARTSPEC", args).map(parsePartitionSpec)
        AlterTableTouch(tableIdent, part)(node.source)

      // ALTER TABLE table_name [PARTITION spec] COMPACT 'compaction_type';
      case Token("TOK_ALTERTABLE_COMPACT", Token(compactType, Nil) :: Nil) :: _ =>
        AlterTableCompact(tableIdent, partition, cleanAndUnquoteString(compactType))(node.source)

      // ALTER TABLE table_name [PARTITION spec] CONCATENATE;
      case Token("TOK_ALTERTABLE_MERGEFILES", _) :: _ =>
        AlterTableMerge(tableIdent, partition)(node.source)

      // ALTER TABLE table_name [PARTITION spec] CHANGE [COLUMN] col_old_name col_new_name
      // column_type [COMMENT col_comment] [FIRST|AFTER column_name] [CASCADE|RESTRICT];
      case Token("TOK_ALTERTABLE_RENAMECOL", oldName :: newName :: dataType :: args) :: _ =>
        val afterColName: Option[String] =
          getClauseOption("TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION", args).map { ap =>
            ap.children match {
              case Token(col, Nil) :: Nil => col
              case _ => parseFailed("Invalid ALTER TABLE command", node)
            }
          }
        val restrict = getClauseOption("TOK_RESTRICT", args).isDefined
        val cascade = getClauseOption("TOK_CASCADE", args).isDefined
        val comment = args.headOption.map {
          case Token("TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION", _) => null
          case Token("TOK_RESTRICT", _) => null
          case Token("TOK_CASCADE", _) => null
          case Token(commentStr, Nil) => cleanAndUnquoteString(commentStr)
          case _ => parseFailed("Invalid ALTER TABLE command", node)
        }
        AlterTableChangeCol(
          tableIdent,
          partition,
          oldName.text,
          newName.text,
          nodeToDataType(dataType),
          comment,
          afterColName,
          restrict,
          cascade)(node.source)

      // ALTER TABLE table_name [PARTITION spec] ADD COLUMNS (name type [COMMENT comment], ...)
      // [CASCADE|RESTRICT]
      case Token("TOK_ALTERTABLE_ADDCOLS", args) :: _ =>
        val columnNodes = getClause("TOK_TABCOLLIST", args).children
        val columns = StructType(columnNodes.map(nodeToStructField))
        val restrict = getClauseOption("TOK_RESTRICT", args).isDefined
        val cascade = getClauseOption("TOK_CASCADE", args).isDefined
        AlterTableAddCol(tableIdent, partition, columns, restrict, cascade)(node.source)

      // ALTER TABLE table_name [PARTITION spec] REPLACE COLUMNS (name type [COMMENT comment], ...)
      // [CASCADE|RESTRICT]
      case Token("TOK_ALTERTABLE_REPLACECOLS", args) :: _ =>
        val columnNodes = getClause("TOK_TABCOLLIST", args).children
        val columns = StructType(columnNodes.map(nodeToStructField))
        val restrict = getClauseOption("TOK_RESTRICT", args).isDefined
        val cascade = getClauseOption("TOK_CASCADE", args).isDefined
        AlterTableReplaceCol(tableIdent, partition, columns, restrict, cascade)(node.source)

      case _ =>
        parseFailed("Unsupported ALTER TABLE command", node)
    }
  }

}
