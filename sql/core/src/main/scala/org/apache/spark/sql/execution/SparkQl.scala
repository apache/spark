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
package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalyst.{CatalystQl, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.catalyst.parser.{ASTNode, ParserConf, SimpleParserConf}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OneRowRelation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StructType

private[sql] class SparkQl(conf: ParserConf = SimpleParserConf()) extends CatalystQl(conf) {
  /** Check if a command should not be explained. */
  protected def isNoExplainCommand(command: String): Boolean =
    "TOK_DESCTABLE" == command || "TOK_ALTERTABLE" == command

  protected override def nodeToPlan(node: ASTNode): LogicalPlan = {
    node match {
      case Token("TOK_SETCONFIG", Nil) =>
        val keyValueSeparatorIndex = node.remainder.indexOf('=')
        if (keyValueSeparatorIndex >= 0) {
          val key = node.remainder.substring(0, keyValueSeparatorIndex).trim
          val value = node.remainder.substring(keyValueSeparatorIndex + 1).trim
          SetCommand(Some(key -> Option(value)))
        } else if (node.remainder.nonEmpty) {
          SetCommand(Some(node.remainder -> None))
        } else {
          SetCommand(None)
        }

      // Just fake explain for any of the native commands.
      case Token("TOK_EXPLAIN", explainArgs) if isNoExplainCommand(explainArgs.head.text) =>
        ExplainCommand(OneRowRelation)

      case Token("TOK_EXPLAIN", explainArgs) if "TOK_CREATETABLE" == explainArgs.head.text =>
        val Some(crtTbl) :: _ :: extended :: Nil =
          getClauses(Seq("TOK_CREATETABLE", "FORMATTED", "EXTENDED"), explainArgs)
        ExplainCommand(nodeToPlan(crtTbl), extended = extended.isDefined)

      case Token("TOK_EXPLAIN", explainArgs) if "TOK_QUERY" == explainArgs.head.text =>
        // Ignore FORMATTED if present.
        val Some(query) :: _ :: extended :: Nil =
          getClauses(Seq("TOK_QUERY", "FORMATTED", "EXTENDED"), explainArgs)
        ExplainCommand(nodeToPlan(query), extended = extended.isDefined)

      case Token("TOK_REFRESHTABLE", nameParts :: Nil) =>
        val tableIdent = extractTableIdent(nameParts)
        RefreshTable(tableIdent)

      case Token("TOK_CREATEDATABASE", Token(databaseName, Nil) :: createDatabaseArgs) =>
        val Seq(
          allowExisting,
          dbLocation,
          databaseComment,
          dbprops) = getClauses(Seq(
          "TOK_IFNOTEXISTS",
          "TOK_DATABASELOCATION",
          "TOK_DATABASECOMMENT",
          "TOK_DATABASEPROPERTIES"), createDatabaseArgs)

        val location = dbLocation.map {
          case Token("TOK_DATABASELOCATION", Token(loc, Nil) :: Nil) => unquoteString(loc)
        }
        val comment = databaseComment.map {
          case Token("TOK_DATABASECOMMENT", Token(comment, Nil) :: Nil) => unquoteString(comment)
        }
        val props: Map[String, String] = dbprops.toSeq.flatMap {
          case Token("TOK_DATABASEPROPERTIES", propList) =>
            propList.flatMap {
              case Token("TOK_DBPROPLIST", props) =>
                props.map {
                  case Token("TOK_TABLEPROPERTY", keysAndValue) =>
                    val key = keysAndValue.init.map(x => unquoteString(x.text)).mkString(".")
                    val value = unquoteString(keysAndValue.last.text)
                    (key, value)
                }
            }
        }.toMap

        CreateDataBase(databaseName, allowExisting.isDefined, location, comment, props)(node.source)

      case Token("TOK_CREATEFUNCTION", func :: as :: createFuncArgs) =>
        val funcName = func.map(x => unquoteString(x.text)).mkString(".")
        val asName = unquoteString(as.text)
        val Seq(
          rList,
          temp) = getClauses(Seq(
          "TOK_RESOURCE_LIST",
          "TOK_TEMPORARY"), createFuncArgs)

        val resourcesMap: Map[String, String] = rList.toSeq.flatMap {
          case Token("TOK_RESOURCE_LIST", resources) =>
            resources.map {
              case Token("TOK_RESOURCE_URI", rType :: Token(rPath, Nil) :: Nil) =>
                val resourceType = rType match {
                  case Token("TOK_JAR", Nil) => "jar"
                  case Token("TOK_FILE", Nil) => "file"
                  case Token("TOK_ARCHIVE", Nil) => "archive"
                }
                (resourceType, unquoteString(rPath))
            }
        }.toMap
        CreateFunction(funcName, asName, resourcesMap, temp.isDefined)(node.source)

      case Token("TOK_ALTERTABLE", alterTableArgs) =>
        val tabName = getClause("TOK_TABNAME", alterTableArgs)
        val rename = getClauseOption("TOK_ALTERTABLE_RENAME", alterTableArgs)
        val setTableProps = getClauseOption("TOK_ALTERTABLE_PROPERTIES", alterTableArgs)
        val dropTableProps = getClauseOption("TOK_ALTERTABLE_DROPPROPERTIES", alterTableArgs)
        val serde = getClauseOption("TOK_ALTERTABLE_SERIALIZER", alterTableArgs)
        val serdeProps = getClauseOption("TOK_ALTERTABLE_SERDEPROPERTIES", alterTableArgs)
        val partitionSpec = getClauseOption("TOK_PARTSPEC", alterTableArgs)
        val bucketSpec = getClauseOption("TOK_ALTERTABLE_CLUSTER_SORT", alterTableArgs)
        val bucketNum = getClauseOption("TOK_ALTERTABLE_BUCKETS", alterTableArgs)
        val tableSkewed = getClauseOption("TOK_ALTERTABLE_SKEWED", alterTableArgs)
        val tableSkewedLocation = getClauseOption("TOK_ALTERTABLE_SKEWED_LOCATION", alterTableArgs)
        val addParts = getClauseOption("TOK_ALTERTABLE_ADDPARTS", alterTableArgs)
        val renamePart = getClauseOption("TOK_ALTERTABLE_RENAMEPART", alterTableArgs)
        val exchangePart = getClauseOption("TOK_ALTERTABLE_EXCHANGEPARTITION", alterTableArgs)
        val dropParts = getClauseOption("TOK_ALTERTABLE_DROPPARTS", alterTableArgs)
        val archivePart = getClauseOption("TOK_ALTERTABLE_ARCHIVE", alterTableArgs)
        val unarchivePart = getClauseOption("TOK_ALTERTABLE_UNARCHIVE", alterTableArgs)
        val setFileFormat = getClauseOption("TOK_ALTERTABLE_FILEFORMAT", alterTableArgs)
        val setLocation = getClauseOption("TOK_ALTERTABLE_LOCATION", alterTableArgs)
        val touch = getClauseOption("TOK_ALTERTABLE_TOUCH", alterTableArgs)
        val compact = getClauseOption("TOK_ALTERTABLE_COMPACT", alterTableArgs)
        val merge = getClauseOption("TOK_ALTERTABLE_MERGEFILES", alterTableArgs)
        val renameCol = getClauseOption("TOK_ALTERTABLE_RENAMECOL", alterTableArgs)
        val addCol = getClauseOption("TOK_ALTERTABLE_ADDCOLS", alterTableArgs)
        val replaceCol = getClauseOption("TOK_ALTERTABLE_REPLACECOLS", alterTableArgs)

        val tableIdent: TableIdentifier = extractTableIdent(tabName)

        def parsePartitionSpec(node: ASTNode): Option[Map[String, Option[String]]] = {
          node match {
            case Token("TOK_PARTSPEC", partitions) =>
              val spec = partitions.map {
                case Token("TOK_PARTVAL", ident :: constant :: Nil) =>
                  (cleanIdentifier(ident.text), Some(cleanIdentifier(constant.text)))
                case Token("TOK_PARTVAL", ident :: Nil) =>
                  (cleanIdentifier(ident.text), None)
              }.toMap
              Some(spec)
            case _ => None
          }
        }

        // Partition Spec
        val partition: Option[Map[String, Option[String]]] =
          partitionSpec.flatMap(parsePartitionSpec)

        def extractTableProps(node: ASTNode): Map[String, Option[String]] = node match {
          case Token("TOK_TABLEPROPERTIES", propsList) =>
            propsList.flatMap {
              case Token("TOK_TABLEPROPLIST", props) =>
                props.map {
                  case Token("TOK_TABLEPROPERTY", key :: Token("TOK_NULL", Nil) :: Nil) =>
                    val k = unquoteString(key.text)
                    (k, None)
                  case Token("TOK_TABLEPROPERTY", key :: value :: Nil) =>
                    val k = unquoteString(key.text)
                    val v = unquoteString(value.text)
                    (k, Some(v))
                }
            }.toMap
        }

        if (rename.isDefined) {
          // Rename table
          val renamedTable = rename.map {
            case Token("TOK_ALTERTABLE_RENAME", renameArgs) =>
              getClause("TOK_TABNAME", renameArgs)
          }
          val renamedTableIdent: Option[TableIdentifier] = renamedTable.map(extractTableIdent)

          AlterTableRename(tableIdent, renamedTableIdent)(node.source)

        } else if (setTableProps.isDefined || dropTableProps.isDefined) {
          // Alter table properties
          val setTableProperties = setTableProps.map {
            case Token("TOK_ALTERTABLE_PROPERTIES", args :: Nil) => extractTableProps(args)
          }
          val dropTableProperties = dropTableProps.map {
            case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) =>
              extractTableProps(args.head)
          }
          val allowExisting = dropTableProps.flatMap {
            case Token("TOK_ALTERTABLE_DROPPROPERTIES", args) =>
              getClauseOption("TOK_IFEXISTS", args)
          }

          AlterTableProperties(
            tableIdent,
            setTableProperties,
            dropTableProperties,
            allowExisting.isDefined)(node.source)
        } else if (serde.isDefined || serdeProps.isDefined) {
          val serdeArgs: Option[Seq[ASTNode]] = serde.map {
            case Token("TOK_ALTERTABLE_SERIALIZER", args) =>
              args
          }

          val serdeClassName = serdeArgs.map(_.head).map {
            case Token(className, Nil) => className
          }

          val serdeProperties: Option[Map[String, Option[String]]] = Option(
            // SET SERDE serde_classname WITH SERDEPROPERTIES
            serdeArgs.map(_.tail).map { props =>
              if (props.isEmpty) {
                null
              } else {
                extractTableProps(props.head)
              }
            }.getOrElse {
              // SET SERDEPROPERTIES
              serdeProps.map {
                case Token("TOK_ALTERTABLE_SERDEPROPERTIES", args) =>
                  extractTableProps(args.head)
              }.getOrElse(null)
            }
          )

          AlterTableSerDeProperties(
            tableIdent,
            serdeClassName,
            serdeProperties,
            partition)(node.source)
        } else if (bucketSpec.isDefined) {
          val (buckets, noClustered, noSorted) = bucketSpec.map {
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
          }.getOrElse((None, false, false)) // should not reach here

          AlterTableStoreProperties(
            tableIdent,
            buckets,
            noClustered,
            noSorted)(node.source)
        } else if (bucketNum.isDefined) {
          val num = bucketNum.get.children.head.text.toInt
          val buckets = Some(BucketSpec(num, Nil, Nil, Nil))
          AlterTableStoreProperties(
            tableIdent,
            buckets,
            false,
            false)(node.source)
        } else if (tableSkewed.isDefined) {
          // Alter Table not skewed
          // Token("TOK_ALTERTABLE_SKEWED", Nil) means not skewed.
          val notSkewed = if (tableSkewed.get.children.size == 0) {
            true
          } else {
            false
          }

          val (notStoredAsDirs, skewedArgs) = tableSkewed.map {
            case Token("TOK_ALTERTABLE_SKEWED", Token("TOK_STOREDASDIRS", Nil) :: Nil) =>
              // Alter Table not stored as directories
              (true, None)
            case Token("TOK_ALTERTABLE_SKEWED", skewedArgs :: Nil) =>
              val (cols, values, storedAsDirs) = skewedArgs match {
                case Token("TOK_TABLESKEWED", skewedCols :: skewedValues :: stored) =>
                  val cols = skewedCols.children.map(_.text)
                  val values = skewedValues match {
                    case Token("TOK_TABCOLVALUE", values) => Seq(values.map(_.text))
                    case Token("TOK_TABCOLVALUE_PAIR", pairs) =>
                      pairs.map {
                        case Token("TOK_TABCOLVALUES", values :: Nil) =>
                          values match {
                            case Token("TOK_TABCOLVALUE", vals) => vals.map(_.text)
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
          }.get

          if (skewedArgs.isDefined) {
            AlterTableSkewed(
              tableIdent,
              skewedArgs.get._1,
              skewedArgs.get._2,
              skewedArgs.get._3, notSkewed, notStoredAsDirs)(node.source)
          } else {
            AlterTableSkewed(tableIdent, Nil, Nil, false, notSkewed, notStoredAsDirs)(node.source)
          }
        } else if (tableSkewedLocation.isDefined) {
          val skewedMaps = tableSkewedLocation.get.children(0) match {
            case Token("TOK_SKEWED_LOCATIONS", locationList :: Nil) =>
              locationList match {
                case Token("TOK_SKEWED_LOCATION_LIST", locationMaps) =>
                  locationMaps.map {
                    case Token("TOK_SKEWED_LOCATION_MAP", key :: value :: Nil) =>
                      val k = key match {
                        case Token(const, Nil) => Seq(const)
                        case Token("TOK_TABCOLVALUES", values :: Nil) =>
                          values match {
                            case Token("TOK_TABCOLVALUE", vals) => vals.map(_.text)
                          }
                      }
                      (k, value.text)
                  }.toMap
              }
          }
          AlterTableSkewedLocation(tableIdent, skewedMaps)(node.source)
        } else if (addParts.isDefined) {
          val allowExisting = getClauseOption("TOK_IFNOTEXISTS", addParts.get.children)
          val parts = if (allowExisting.isDefined) {
            addParts.get.children.tail
          } else {
            addParts.get.children
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
        } else if (renamePart.isDefined) {
          val newPartition = parsePartitionSpec(renamePart.get.children(0))
          AlterTableRenamePartition(tableIdent, partition.get, newPartition.get)(node.source)
        } else if (exchangePart.isDefined) {
          val Seq(Some(partSpec), Some(fromTable)) =
            getClauses(Seq("TOK_PARTSPEC", "TOK_TABNAME"), exchangePart.get.children)
          val partition = parsePartitionSpec(partSpec).get
          val fromTableIdent = extractTableIdent(fromTable)
          AlterTableExchangePartition(tableIdent, fromTableIdent, partition)(node.source)
        } else if (dropParts.isDefined) {
          val parts = dropParts.get.children.collect {
            case Token("TOK_PARTSPEC", partitions) =>
              partitions.map {
                case Token("TOK_PARTVAL", ident :: op :: constant :: Nil) =>
                  (cleanIdentifier(ident.text), op.text, cleanIdentifier(constant.text))
              }
          }

          val allowExisting = getClauseOption("TOK_IFEXISTS", dropParts.get.children)
          val purge = getClauseOption("PURGE", dropParts.get.children)

          val replication = getClauseOption("TOK_REPLICATION", dropParts.get.children).map {
            case Token("TOK_REPLICATION", replId :: metadata :: Nil) =>
              (replId.text, true)
            case Token("TOK_REPLICATION", replId :: Nil) =>
              (replId.text, false)
          }

          AlterTableDropPartition(
            tableIdent,
            parts,
            allowExisting.isDefined,
            purge.isDefined,
            replication)(node.source)
        } else if (archivePart.isDefined) {
          val partition = parsePartitionSpec(archivePart.get.children(0)).get
          AlterTableArchivePartition(tableIdent, partition)(node.source)
        } else if (unarchivePart.isDefined) {
          val partition = parsePartitionSpec(unarchivePart.get.children(0)).get
          AlterTableUnarchivePartition(tableIdent, partition)(node.source)
        } else if (setFileFormat.isDefined) {
          val Seq(fileFormat, genericFormat) =
            getClauses(Seq("TOK_TABLEFILEFORMAT", "TOK_FILEFORMAT_GENERIC"),
              setFileFormat.get.children)
          val fFormat = fileFormat.map(_.children.map(_.text))
          val gFormat = genericFormat.map(_.children(0).text)
          AlterTableSetFileFormat(tableIdent, partition, fFormat, gFormat)(node.source)
        } else if (setLocation.isDefined) {
          val loc = cleanIdentifier(setLocation.get.children(0).text)
          AlterTableSetLocation(tableIdent, partition, loc)(node.source)
        } else if (touch.isDefined) {
          val part = getClauseOption("TOK_PARTSPEC", touch.get.children).flatMap(parsePartitionSpec)
          AlterTableTouch(tableIdent, part)(node.source)
        } else if (compact.isDefined) {
          val compactType = cleanIdentifier(compact.get.children(0).text)
          AlterTableCompact(tableIdent, partition, compactType)(node.source)
        } else if (merge.isDefined) {
          AlterTableMerge(tableIdent, partition)(node.source)
        }
        else if (renameCol.isDefined) {
          val oldName = renameCol.get.children(0).text
          val newName = renameCol.get.children(1).text
          val dataType = nodeToDataType(renameCol.get.children(2))
          val afterPos =
            getClauseOption("TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION", renameCol.get.children)
          val afterPosCol = afterPos.map { ap =>
            ap.children match {
              case Token(col, Nil) :: Nil => col
              case _ => null
            }
          }

          val restrict = getClauseOption("TOK_RESTRICT", renameCol.get.children)
          val cascade = getClauseOption("TOK_CASCADE", renameCol.get.children)

          val comment = if (renameCol.get.children.size > 3) {
            renameCol.get.children(3) match {
              case Token(commentStr, Nil)
                if commentStr != "TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION" &&
                  commentStr != "TOK_RESTRICT" && commentStr != "TOK_CASCADE" => Some(commentStr)
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
        } else if (addCol.isDefined || replaceCol.isDefined) {
          val thisNode = if (addCol.isDefined) {
            addCol.get
          } else {
            replaceCol.get
          }

          val tableCols = getClause("TOK_TABCOLLIST", thisNode.children)
          val columns = tableCols match {
            case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
          }

          val restrict = getClauseOption("TOK_RESTRICT", thisNode.children)
          val cascade = getClauseOption("TOK_CASCADE", thisNode.children)

          if (addCol.isDefined) {
            AlterTableAddCol(
              tableIdent,
              partition,
              columns,
              restrict.isDefined,
              cascade.isDefined)(node.source)
          } else {
            AlterTableReplaceCol(
              tableIdent,
              partition,
              columns,
              restrict.isDefined,
              cascade.isDefined)(node.source)
          }
        } else {
          nodeToDescribeFallback(node)
        }

      case Token("TOK_CREATETABLEUSING", createTableArgs) =>
        val Seq(
          temp,
          allowExisting,
          Some(tabName),
          tableCols,
          Some(Token("TOK_TABLEPROVIDER", providerNameParts)),
          tableOpts,
          tableAs) = getClauses(Seq(
          "TEMPORARY",
          "TOK_IFNOTEXISTS",
          "TOK_TABNAME", "TOK_TABCOLLIST",
          "TOK_TABLEPROVIDER",
          "TOK_TABLEOPTIONS",
          "TOK_QUERY"), createTableArgs)

        val tableIdent: TableIdentifier = extractTableIdent(tabName)

        val columns = tableCols.map {
          case Token("TOK_TABCOLLIST", fields) => StructType(fields.map(nodeToStructField))
        }

        val provider = providerNameParts.map {
          case Token(name, Nil) => name
        }.mkString(".")

        val options: Map[String, String] = tableOpts.toSeq.flatMap {
          case Token("TOK_TABLEOPTIONS", options) =>
            options.map {
              case Token("TOK_TABLEOPTION", keysAndValue) =>
                val key = keysAndValue.init.map(_.text).mkString(".")
                val value = unquoteString(keysAndValue.last.text)
                (key, value)
            }
        }.toMap

        val asClause = tableAs.map(nodeToPlan(_))

        if (temp.isDefined && allowExisting.isDefined) {
          throw new AnalysisException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        if (asClause.isDefined) {
          if (columns.isDefined) {
            throw new AnalysisException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }

          val mode = if (allowExisting.isDefined) {
            SaveMode.Ignore
          } else if (temp.isDefined) {
            SaveMode.Overwrite
          } else {
            SaveMode.ErrorIfExists
          }

          CreateTableUsingAsSelect(tableIdent,
            provider,
            temp.isDefined,
            Array.empty[String],
            bucketSpec = None,
            mode,
            options,
            asClause.get)
        } else {
          CreateTableUsing(
            tableIdent,
            columns,
            provider,
            temp.isDefined,
            options,
            allowExisting.isDefined,
            managedIfNoPath = false)
        }

      case Token("TOK_SWITCHDATABASE", Token(database, Nil) :: Nil) =>
        SetDatabaseCommand(cleanIdentifier(database))

      case Token("TOK_DESCTABLE", describeArgs) =>
        // Reference: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
        val Some(tableType) :: formatted :: extended :: pretty :: Nil =
          getClauses(Seq("TOK_TABTYPE", "FORMATTED", "EXTENDED", "PRETTY"), describeArgs)
        if (formatted.isDefined || pretty.isDefined) {
          // FORMATTED and PRETTY are not supported and this statement will be treated as
          // a Hive native command.
          nodeToDescribeFallback(node)
        } else {
          tableType match {
            case Token("TOK_TABTYPE", Token("TOK_TABNAME", nameParts) :: Nil) =>
              nameParts match {
                case Token(dbName, Nil) :: Token(tableName, Nil) :: Nil =>
                  // It is describing a table with the format like "describe db.table".
                  // TODO: Actually, a user may mean tableName.columnName. Need to resolve this
                  // issue.
                  val tableIdent = TableIdentifier(
                    cleanIdentifier(tableName), Some(cleanIdentifier(dbName)))
                  datasources.DescribeCommand(
                    UnresolvedRelation(tableIdent, None), isExtended = extended.isDefined)
                case Token(dbName, Nil) :: Token(tableName, Nil) :: Token(colName, Nil) :: Nil =>
                  // It is describing a column with the format like "describe db.table column".
                  nodeToDescribeFallback(node)
                case tableName :: Nil =>
                  // It is describing a table with the format like "describe table".
                  datasources.DescribeCommand(
                    UnresolvedRelation(TableIdentifier(cleanIdentifier(tableName.text)), None),
                    isExtended = extended.isDefined)
                case _ =>
                  nodeToDescribeFallback(node)
              }
            // All other cases.
            case _ =>
              nodeToDescribeFallback(node)
          }
        }

      case Token("TOK_CACHETABLE", Token(tableName, Nil) :: args) =>
       val Seq(lzy, selectAst) = getClauses(Seq("LAZY", "TOK_QUERY"), args)
        CacheTableCommand(tableName, selectAst.map(nodeToPlan), lzy.isDefined)

      case Token("TOK_UNCACHETABLE", Token(tableName, Nil) :: Nil) =>
        UncacheTableCommand(tableName)

      case Token("TOK_CLEARCACHE", Nil) =>
        ClearCacheCommand

      case Token("TOK_SHOWTABLES", args) =>
        val databaseName = args match {
          case Nil => None
          case Token("TOK_FROM", Token(dbName, Nil) :: Nil) :: Nil => Option(dbName)
          case _ => noParseRule("SHOW TABLES", node)
        }
        ShowTablesCommand(databaseName)

      case _ =>
        super.nodeToPlan(node)
    }
  }

  protected def nodeToDescribeFallback(node: ASTNode): LogicalPlan = noParseRule("Describe", node)
}
