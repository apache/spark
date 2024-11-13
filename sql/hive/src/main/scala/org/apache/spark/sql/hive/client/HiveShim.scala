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

package org.apache.spark.sql.hive.client

import java.lang.{Boolean => JBoolean, Integer => JInteger, Long => JLong}
import java.lang.reflect.{InvocationTargetException, Method}
import java.util.{ArrayList => JArrayList, List => JList, Locale, Map => JMap}
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.{IMetaStoreClient, PartitionDropOptions, TableType}
import org.apache.hadoop.hive.metastore.api.{Database, EnvironmentContext, Function => HiveFunction, FunctionType, Index, MetaException, PrincipalType, ResourceType, ResourceUri}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.io.AcidUtils
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException, Partition, Table}
import org.apache.hadoop.hive.ql.plan.{AddPartitionDesc, DynamicPartitionCtx}
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CONFIG, CONFIG2, CONFIG3}
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.NoSuchPermanentFunctionException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, DateFormatter, TypeUtils}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * A shim that defines the interface between [[HiveClientImpl]] and the underlying Hive library used
 * to talk to the metastore. Each Hive version has its own implementation of this class, defining
 * version-specific version of needed functions.
 *
 * The guideline for writing shims is:
 * - always extend from the previous version unless really not possible
 * - initialize methods in lazy vals, both for quicker access for multiple invocations, and to
 *   avoid runtime errors due to the above guideline.
 */
private[client] sealed abstract class Shim {

  /**
   * Set the current SessionState to the given SessionState. Also, set the context classloader of
   * the current thread to the one set in the HiveConf of this given `state`.
   */
  def setCurrentSessionState(state: SessionState): Unit

  /**
   * This shim is necessary because the return type is different on different versions of Hive.
   * All parameters are the same, though.
   */
  def getDataLocation(table: Table): Option[String]

  def createDatabase(hive: Hive, db: Database, ignoreIfExists: Boolean): Unit

  def dropDatabase(
      hive: Hive,
      dbName: String,
      deleteData: Boolean,
      ignoreUnknownDb: Boolean,
      cascade: Boolean): Unit

  def alterDatabase(hive: Hive, dbName: String, d: Database): Unit

  def getDatabase(hive: Hive, dbName: String): Database

  def getAllDatabases(hive: Hive): Seq[String]

  def getDatabasesByPattern(hive: Hive, pattern: String): Seq[String]

  def databaseExists(hive: Hive, dbName: String): Boolean

  def setDataLocation(table: Table, loc: String): Unit

  def getAllPartitions(hive: Hive, table: Table): Seq[Partition]

  def getPartitionsByFilter(
      hive: Hive,
      table: Table,
      predicates: Seq[Expression],
      catalogTable: CatalogTable): Seq[Partition]

  def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor

  def getDriverResults(driver: Driver): Seq[String]

  def getMetastoreClientConnectRetryDelayMillis(conf: HiveConf): Long

  def alterTable(hive: Hive, tableName: String, table: Table): Unit

  def alterPartitions(hive: Hive, tableName: String, newParts: JList[Partition]): Unit

  def createTable(hive: Hive, table: Table, ifNotExists: Boolean): Unit

  def getTable(
      hive: Hive,
      dbName: String,
      tableName: String,
      throwException: Boolean = true): Table

  def getTablesByType(
      hive: Hive,
      dbName: String,
      pattern: String,
      tableType: TableType): Seq[String]

  def getTablesByPattern(hive: Hive, dbName: String, pattern: String): Seq[String]

  def getAllTables(hive: Hive, dbName: String): Seq[String]

  def dropTable(hive: Hive, dbName: String, tableName: String): Unit

  def getPartition(
      hive: Hive,
      table: Table,
      partSpec: JMap[String, String],
      forceCreate: Boolean): Partition

  def getPartitions(
      hive: Hive,
      table: Table,
      partSpec: JMap[String, String]): Seq[Partition]

  def getPartitionNames(
      hive: Hive,
      dbName: String,
      tableName: String,
      max: Short): Seq[String]

  def getPartitionNames(
      hive: Hive,
      dbName: String,
      tableName: String,
      partSpec: JMap[String, String],
      max: Short): Seq[String]

  def createPartitions(
      hive: Hive,
      table: Table,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit

  def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean,
      isSrcLocal: Boolean): Unit

  def renamePartition(
      hive: Hive,
      table: Table,
      oldPartSpec: JMap[String, String],
      newPart: Partition): Unit

  def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit

  def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      hiveTable: Table): Unit

  def createFunction(hive: Hive, db: String, func: CatalogFunction): Unit

  def dropFunction(hive: Hive, db: String, name: String): Unit

  def renameFunction(hive: Hive, db: String, oldName: String, newName: String): Unit

  def alterFunction(hive: Hive, db: String, func: CatalogFunction): Unit

  def getFunctionOption(hive: Hive, db: String, name: String): Option[CatalogFunction]

  def listFunctions(hive: Hive, db: String, pattern: String): Seq[String]

  def dropIndex(hive: Hive, dbName: String, tableName: String, indexName: String): Unit

  def dropTable(
      hive: Hive,
      dbName: String,
      tableName: String,
      deleteData: Boolean,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit

  def dropPartition(
      hive: Hive,
      dbName: String,
      tableName: String,
      part: JList[String],
      deleteData: Boolean,
      purge: Boolean): Unit

  def getDatabaseOwnerName(db: Database): String

  def setDatabaseOwnerName(db: Database, owner: String): Unit

  def getMSC(hive: Hive): IMetaStoreClient

  def getIndexes(hive: Hive, dbName: String, tableName: String, max: Short): Seq[Index]

  protected def findMethod(klass: Class[_], name: String, args: Class[_]*): Method = {
    klass.getMethod(name, args: _*)
  }

  // This method should be called before a Hive client call is made.
  def recordHiveCall(): Unit = {
    HiveCatalogMetrics.incrementHiveClientCalls(1)
  }
}

private[client] class Shim_v2_0 extends Shim with Logging {
  // deletes the underlying data along with metadata
  protected lazy val deleteDataInDropIndex = JBoolean.TRUE
  // true if this is an ACID operation
  protected lazy val isAcid = JBoolean.FALSE
  // true if list bucketing enabled
  protected lazy val isSkewedStoreAsSubdir = JBoolean.FALSE
  // throws an exception if the index does not exist
  protected lazy val throwExceptionInDropIndex = JBoolean.TRUE
  // txnId can be 0 unless isAcid == true
  protected lazy val txnIdInLoadDynamicPartitions: JLong = 0L

  protected lazy val wildcard: String = ".*"

  override def getMSC(hive: Hive): IMetaStoreClient = hive.getMSC

  private lazy val loadPartitionMethod =
    findMethod(
      classOf[Hive],
      "loadPartition",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadTableMethod =
    findMethod(
      classOf[Hive],
      "loadTable",
      classOf[Path],
      classOf[String],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JLong.TYPE)
  private lazy val alterTableMethod =
    findMethod(
      classOf[Hive],
      "alterTable",
      classOf[String],
      classOf[Table])
  private lazy val alterPartitionsMethod =
    findMethod(
      classOf[Hive],
      "alterPartitions",
      classOf[String],
      classOf[JList[Partition]])

  override def setCurrentSessionState(state: SessionState): Unit =
    SessionState.setCurrentSessionState(state)

  override def getDataLocation(table: Table): Option[String] =
    Option(table.getDataLocation).map(_.toString())

  override def setDataLocation(table: Table, loc: String): Unit =
    table.setDataLocation(new Path(loc))

  override def createPartitions(
      hive: Hive,
      table: Table,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val addPartitionDesc = new AddPartitionDesc(table.getDbName, table.getTableName, ignoreIfExists)
    parts.zipWithIndex.foreach { case (s, i) =>
      addPartitionDesc.addPartition(
        s.spec.asJava, s.storage.locationUri.map(CatalogUtils.URIToString).orNull)
      if (s.parameters.nonEmpty) {
        addPartitionDesc.getPartition(i).setPartParams(s.parameters.asJava)
      }
    }
    recordHiveCall()
    hive.createPartitions(addPartitionDesc)
  }

  override def getAllPartitions(hive: Hive, table: Table): Seq[Partition] = {
    recordHiveCall()
    hive.getAllPartitionsOf(table).asScala.toSeq
  }

  override def getPartitionsByFilter(
      hive: Hive,
      table: Table,
      predicates: Seq[Expression],
      catalogTable: CatalogTable): Seq[Partition] = {
    // Hive getPartitionsByFilter() takes a string that represents partition
    // predicates like "str_key=\"value\" and int_key=1 ..."
    val filter = convertFilters(table, predicates)

    val partitions =
      if (filter.isEmpty) {
        prunePartitionsFastFallback(hive, table, catalogTable, predicates)
      } else {
        logDebug(s"Hive metastore filter is '$filter'.")
        val tryDirectSqlConfVar = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL
        val shouldFallback = SQLConf.get.metastorePartitionPruningFallbackOnException
        try {
          // Hive may throw an exception when calling this method in some circumstances, such as
          // when filtering on a non-string partition column when the hive config key
          // hive.metastore.try.direct.sql is false. In some cases the remote metastore will throw
          // exceptions even if the config is true, due to various reasons including the
          // underlying RDBMS, Hive bugs when generating the filter, etc.
          //
          // Because of the above we'll fallback to use `Hive.getAllPartitionsOf` when the exception
          // occurs and the config`spark.sql.hive.metastorePartitionPruningFallbackOnException` is
          // enabled.
          recordHiveCall()
          hive.getPartitionsByFilter(table, filter)
        } catch {
          case ex: MetaException if shouldFallback =>
            logWarning(log"Caught Hive MetaException attempting to get partition metadata by " +
              log"filter from Hive. Falling back to fetching all partition metadata, which will " +
              log"degrade performance. Modifying your Hive metastore configuration to set " +
              log"${MDC(CONFIG, tryDirectSqlConfVar.varname)} to true " +
              log"(if it is not true already) may resolve this problem. Or you can enable " +
              log"${MDC(CONFIG2, SQLConf.HIVE_METASTORE_PARTITION_PRUNING_FAST_FALLBACK.key)} " +
              log"to alleviate performance downgrade. Otherwise, to avoid degraded performance " +
              log"you can set ${MDC(CONFIG3,
                SQLConf.HIVE_METASTORE_PARTITION_PRUNING_FALLBACK_ON_EXCEPTION.key)} " +
              log"to false and let the query fail instead.", ex)
            // HiveShim clients are expected to handle a superset of the requested partitions
            prunePartitionsFastFallback(hive, table, catalogTable, predicates)
          case ex: MetaException =>
            throw QueryExecutionErrors.getPartitionMetadataByFilterError(ex)
        }
      }

    partitions.asScala.toSeq
  }

  private def prunePartitionsFastFallback(
      hive: Hive,
      table: Table,
      catalogTable: CatalogTable,
      predicates: Seq[Expression]): java.util.Collection[Partition] = {
    val timeZoneId = SQLConf.get.sessionLocalTimeZone

    // Because there is no way to know whether the partition properties has timeZone,
    // client-side filtering cannot be used with TimeZoneAwareExpression.
    def hasTimeZoneAwareExpression(e: Expression): Boolean = {
      e.exists {
        case cast: Cast => cast.needsTimeZone
        case tz: TimeZoneAwareExpression => !tz.isInstanceOf[Cast]
        case _ => false
      }
    }

    if (!SQLConf.get.metastorePartitionPruningFastFallback ||
      predicates.isEmpty ||
      predicates.exists(hasTimeZoneAwareExpression)) {
      recordHiveCall()
      hive.getAllPartitionsOf(table)
    } else {
      try {
        val partitionSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
          catalogTable.partitionSchema)
        val boundPredicate = ExternalCatalogUtils.generatePartitionPredicateByFilter(
          catalogTable, partitionSchema, predicates)

        def toRow(spec: TablePartitionSpec): InternalRow = {
          InternalRow.fromSeq(partitionSchema.map { field =>
            val partValue = if (spec(field.name) == ExternalCatalogUtils.DEFAULT_PARTITION_NAME) {
              null
            } else {
              spec(field.name)
            }
            Cast(Literal(partValue), field.dataType, Option(timeZoneId)).eval()
          })
        }

        recordHiveCall()
        val allPartitionNames = hive.getPartitionNames(
          table.getDbName, table.getTableName, -1).asScala
        val partNames = allPartitionNames.filter { p =>
          val spec = PartitioningUtils.parsePathFragment(p)
          boundPredicate.eval(toRow(spec))
        }
        recordHiveCall()
        hive.getPartitionsByNames(table, partNames.asJava)
      } catch {
        case ex: HiveException if ex.getCause.isInstanceOf[MetaException] =>
          logWarning("Caught Hive MetaException attempting to get partition metadata by " +
            "filter from client side. Falling back to fetching all partition metadata", ex)
          recordHiveCall()
          hive.getAllPartitionsOf(table)
      }
    }
  }

  override def getCommandProcessor(token: String, conf: HiveConf): CommandProcessor =
    CommandProcessorFactory.get(Array(token), conf)

  override def getDriverResults(driver: Driver): Seq[String] = {
    val res = new JArrayList[Object]()
    driver.getResults(res)
    res.asScala.map {
      case s: String => s
      case a: Array[Object] => a(0).asInstanceOf[String]
    }.toSeq
  }

  override def getMetastoreClientConnectRetryDelayMillis(conf: HiveConf): Long =
    conf.getTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.MILLISECONDS)

  override def getTablesByType(
      hive: Hive,
      dbName: String,
      pattern: String,
      tableType: TableType): Seq[String] = {
    throw QueryExecutionErrors.getTablesByTypeUnsupportedByHiveVersionError()
  }

  override def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean,
      isSrcLocal: Boolean): Unit = {
    recordHiveCall()
    loadPartitionMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      inheritTableSpecs: JBoolean, isSkewedStoreAsSubdir: JBoolean,
      isSrcLocal: JBoolean, isAcid)
  }

  override def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit = {
    recordHiveCall()
    loadTableMethod.invoke(hive, loadPath, tableName, replace: JBoolean, isSrcLocal: JBoolean,
      isSkewedStoreAsSubdir, isAcid)
  }

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      hiveTable: Table): Unit = {
    recordHiveCall()
    val listBucketingEnabled = hiveTable.isStoredAsSubDirectories
    loadDynamicPartitionsMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      numDP: JInteger, listBucketingEnabled: JBoolean, isAcid, txnIdInLoadDynamicPartitions)
  }

  override def dropIndex(hive: Hive, dbName: String, tableName: String, indexName: String): Unit = {
    recordHiveCall()
    hive.dropIndex(dbName, tableName, indexName, throwExceptionInDropIndex, deleteDataInDropIndex)
  }

  override def dropTable(
      hive: Hive,
      dbName: String,
      tableName: String,
      deleteData: Boolean,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    recordHiveCall()
    hive.dropTable(dbName, tableName, deleteData, ignoreIfNotExists, purge)
  }

  override def alterTable(hive: Hive, tableName: String, table: Table): Unit = {
    recordHiveCall()
    alterTableMethod.invoke(hive, tableName, table)
  }

  override def alterPartitions(hive: Hive, tableName: String, newParts: JList[Partition]): Unit = {
    recordHiveCall()
    alterPartitionsMethod.invoke(hive, tableName, newParts)
  }

  override def dropPartition(
      hive: Hive,
      dbName: String,
      tableName: String,
      part: JList[String],
      deleteData: Boolean,
      purge: Boolean): Unit = {
    val dropOptions = new PartitionDropOptions
    dropOptions.deleteData(deleteData)
    dropOptions.purgeData(purge)
    recordHiveCall()
    hive.dropPartition(dbName, tableName, part, dropOptions)
  }

  private def toHiveFunction(f: CatalogFunction, db: String): HiveFunction = {
    val resourceUris = f.resources.map { resource =>
      new ResourceUri(ResourceType.valueOf(
        resource.resourceType.resourceType.toUpperCase(Locale.ROOT)), resource.uri)
    }
    new HiveFunction(
      f.identifier.funcName,
      db,
      f.className,
      null,
      PrincipalType.USER,
      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis).toInt,
      FunctionType.JAVA,
      resourceUris.asJava)
  }

  override def createFunction(hive: Hive, db: String, func: CatalogFunction): Unit = {
    recordHiveCall()
    hive.createFunction(toHiveFunction(func, db))
  }

  override def dropFunction(hive: Hive, db: String, name: String): Unit = {
    recordHiveCall()
    hive.dropFunction(db, name)
  }

  override def renameFunction(hive: Hive, db: String, oldName: String, newName: String): Unit = {
    val catalogFunc = getFunctionOption(hive, db, oldName)
      .getOrElse(throw new NoSuchPermanentFunctionException(db, oldName))
      .copy(identifier = FunctionIdentifier(newName, Some(db)))
    val hiveFunc = toHiveFunction(catalogFunc, db)
    recordHiveCall()
    hive.alterFunction(db, oldName, hiveFunc)
  }

  override def alterFunction(hive: Hive, db: String, func: CatalogFunction): Unit = {
    recordHiveCall()
    hive.alterFunction(db, func.identifier.funcName, toHiveFunction(func, db))
  }

  private def fromHiveFunction(hf: HiveFunction): CatalogFunction = {
    val name = FunctionIdentifier(hf.getFunctionName, Option(hf.getDbName))
    val resources = hf.getResourceUris.asScala.map { uri =>
      val resourceType = uri.getResourceType match {
        case ResourceType.ARCHIVE => "archive"
        case ResourceType.FILE => "file"
        case ResourceType.JAR => "jar"
        case r => throw QueryCompilationErrors.unknownHiveResourceTypeError(r.toString)
      }
      FunctionResource(FunctionResourceType.fromString(resourceType), uri.getUri)
    }
    CatalogFunction(name, hf.getClassName, resources.toSeq)
  }

  override def getFunctionOption(hive: Hive, db: String, name: String): Option[CatalogFunction] = {
    try {
      recordHiveCall()
      Option(hive.getFunction(db, name)).map(fromHiveFunction)
    } catch {
      case NonFatal(e) if isCausedBy(e, s"$name does not exist") =>
        None
    }
  }

  @scala.annotation.tailrec
  private def isCausedBy(e: Throwable, matchMassage: String): Boolean = {
    if (e.getMessage.contains(matchMassage)) {
      true
    } else if (e.getCause != null) {
      isCausedBy(e.getCause, matchMassage)
    } else {
      false
    }
  }

  override def listFunctions(hive: Hive, db: String, pattern: String): Seq[String] = {
    recordHiveCall()
    hive.getFunctions(db, pattern).asScala.toSeq
  }

  /**
   * Converts catalyst expression to the format that Hive's getPartitionsByFilter() expects, i.e.
   * a string that represents partition predicates like "str_key=\"value\" and int_key=1 ...".
   *
   * Unsupported predicates are skipped.
   */
  def convertFilters(table: Table, filters: Seq[Expression]): String = {
    lazy val dateFormatter = DateFormatter()

    /**
     * An extractor that matches all binary comparison operators except null-safe equality.
     *
     * Null-safe equality is not supported by Hive metastore partition predicate pushdown
     */
    object SpecialBinaryComparison {
      def unapply(e: BinaryComparison): Option[(Expression, Expression)] = e match {
        case _: EqualNullSafe => None
        case _ => Some((e.left, e.right))
      }
    }

    object ExtractableLiteral {
      def unapply(expr: Expression): Option[String] = expr match {
        case Literal(null, _) => None // `null`s can be cast as other types; we want to avoid NPEs.
        case Literal(value, _: IntegralType) => Some(value.toString)
        case Literal(value, _: StringType) => Some(quoteStringLiteral(value.toString))
        case Literal(value, _: DateType) =>
          Some(quoteStringLiteral(dateFormatter.format(value.asInstanceOf[Int])))
        case _ => None
      }
    }

    object ExtractableLiterals {
      def unapply(exprs: Seq[Expression]): Option[Seq[String]] = {
        // SPARK-24879: The Hive metastore filter parser does not support "null", but we still want
        // to push down as many predicates as we can while still maintaining correctness.
        // In SQL, the `IN` expression evaluates as follows:
        //  > `1 in (2, NULL)` -> NULL
        //  > `1 in (1, NULL)` -> true
        //  > `1 in (2)` -> false
        // Since Hive metastore filters are NULL-intolerant binary operations joined only by
        // `AND` and `OR`, we can treat `NULL` as `false` and thus rewrite `1 in (2, NULL)` as
        // `1 in (2)`.
        // If the Hive metastore begins supporting NULL-tolerant predicates and Spark starts
        // pushing down these predicates, then this optimization will become incorrect and need
        // to be changed.
        val extractables = exprs
          .filter {
            case Literal(null, _) => false
            case _ => true
          }.map(ExtractableLiteral.unapply)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object ExtractableValues {
      private lazy val valueToLiteralString: PartialFunction[Any, String] = {
        case value: Byte => value.toString
        case value: Short => value.toString
        case value: Int => value.toString
        case value: Long => value.toString
        case value: UTF8String => quoteStringLiteral(value.toString)
      }

      def unapply(values: Set[Any]): Option[Seq[String]] = {
        val extractables = values.filter(_ != null).toSeq.map(valueToLiteralString.lift)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object ExtractableDateValues {
      private lazy val valueToLiteralString: PartialFunction[Any, String] = {
        case value: Int => quoteStringLiteral(dateFormatter.format(value))
      }

      def unapply(values: Set[Any]): Option[Seq[String]] = {
        val extractables = values.filter(_ != null).toSeq.map(valueToLiteralString.lift)
        if (extractables.nonEmpty && extractables.forall(_.isDefined)) {
          Some(extractables.map(_.get))
        } else {
          None
        }
      }
    }

    object SupportedAttribute {
      // hive varchar is treated as catalyst string, but hive varchar can't be pushed down.
      private val varcharKeys = table.getPartitionKeys.asScala
        .filter(col => col.getType.startsWith(serdeConstants.VARCHAR_TYPE_NAME) ||
          col.getType.startsWith(serdeConstants.CHAR_TYPE_NAME))
        .map(col => col.getName).toSet

      def unapply(attr: Attribute): Option[String] = {
        val resolver = SQLConf.get.resolver
        if (varcharKeys.exists(c => resolver(c, attr.name))) {
          None
        } else if (attr.dataType.isInstanceOf[IntegralType] || attr.dataType == StringType ||
            attr.dataType == DateType) {
          Some(attr.name)
        } else {
          None
        }
      }
    }

    def convertInToOr(name: String, values: Seq[String]): String = {
      values.map(value => s"$name = $value").mkString("(", " or ", ")")
    }

    def convertNotInToAnd(name: String, values: Seq[String]): String = {
      values.map(value => s"$name != $value").mkString("(", " and ", ")")
    }

    def hasNullLiteral(list: Seq[Expression]): Boolean = list.exists {
      case Literal(null, _) => true
      case _ => false
    }

    val useAdvanced = SQLConf.get.advancedPartitionPredicatePushdownEnabled
    val inSetThreshold = SQLConf.get.metastorePartitionPruningInSetThreshold

    object ExtractAttribute {
      @scala.annotation.tailrec
      def unapply(expr: Expression): Option[Attribute] = {
        expr match {
          case attr: Attribute => Some(attr)
          case Cast(child @ IntegralTypeExpression(), dt: IntegralType, _, _)
              if Cast.canUpCast(child.dataType.asInstanceOf[AtomicType], dt) => unapply(child)
          case _ => None
        }
      }
    }

    def convert(expr: Expression): Option[String] = expr match {
      case Not(InSet(_, values)) if values.size > inSetThreshold =>
        None

      case Not(In(_, list)) if hasNullLiteral(list) => None
      case Not(InSet(_, list)) if list.contains(null) => None

      case In(ExtractAttribute(SupportedAttribute(name)), ExtractableLiterals(values))
          if useAdvanced =>
        Some(convertInToOr(name, values))

      case Not(In(ExtractAttribute(SupportedAttribute(name)), ExtractableLiterals(values)))
          if useAdvanced =>
        Some(convertNotInToAnd(name, values))

      case InSet(child, values) if useAdvanced && values.size > inSetThreshold =>
        val dataType = child.dataType
        // Skip null here is safe, more details could see at ExtractableLiterals.
        val sortedValues = values.filter(_ != null).toSeq
          .sorted(TypeUtils.getInterpretedOrdering(dataType))
        convert(And(GreaterThanOrEqual(child, Literal(sortedValues.head, dataType)),
          LessThanOrEqual(child, Literal(sortedValues.last, dataType))))

      case InSet(child @ ExtractAttribute(SupportedAttribute(name)), ExtractableDateValues(values))
          if useAdvanced && child.dataType == DateType =>
        Some(convertInToOr(name, values))

      case Not(InSet(child @ ExtractAttribute(SupportedAttribute(name)),
        ExtractableDateValues(values))) if useAdvanced && child.dataType == DateType =>
        Some(convertNotInToAnd(name, values))

      case InSet(ExtractAttribute(SupportedAttribute(name)), ExtractableValues(values))
          if useAdvanced =>
        Some(convertInToOr(name, values))

      case Not(InSet(ExtractAttribute(SupportedAttribute(name)), ExtractableValues(values)))
          if useAdvanced =>
        Some(convertNotInToAnd(name, values))

      case op @ SpecialBinaryComparison(
          ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name ${op.symbol} $value")

      case op @ SpecialBinaryComparison(
          ExtractableLiteral(value), ExtractAttribute(SupportedAttribute(name))) =>
        Some(s"$value ${op.symbol} $name")

      case Contains(ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name like " + (("\"" + wildcard + value.drop(1)).dropRight(1) + wildcard + "\""))

      case StartsWith(ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name like " + (value.dropRight(1) + wildcard + "\""))

      case EndsWith(ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value)) =>
        Some(s"$name like " + ("\"" + wildcard + value.drop(1)))

      case And(expr1, expr2) if useAdvanced =>
        val converted = convert(expr1) ++ convert(expr2)
        if (converted.isEmpty) {
          None
        } else {
          Some(converted.mkString("(", " and ", ")"))
        }

      case Or(expr1, expr2) if useAdvanced =>
        for {
          left <- convert(expr1)
          right <- convert(expr2)
        } yield s"($left or $right)"

      case Not(EqualTo(
          ExtractAttribute(SupportedAttribute(name)), ExtractableLiteral(value))) if useAdvanced =>
        Some(s"$name != $value")

      case Not(EqualTo(
          ExtractableLiteral(value), ExtractAttribute(SupportedAttribute(name)))) if useAdvanced =>
        Some(s"$value != $name")

      case _ => None
    }

    filters.flatMap(convert).mkString(" and ")
  }

  private def quoteStringLiteral(str: String): String = {
    if (!str.contains("\"")) {
      s""""$str""""
    } else if (!str.contains("'")) {
      s"""'$str'"""
    } else {
      throw QueryExecutionErrors.invalidPartitionFilterError()
    }
  }

  override def getDatabaseOwnerName(db: Database): String = {
    Option(db.getOwnerName).getOrElse("")
  }

  override def setDatabaseOwnerName(db: Database, owner: String): Unit = {
    db.setOwnerName(owner)
  }

  override def createDatabase(hive: Hive, db: Database, ignoreIfExists: Boolean): Unit = {
    recordHiveCall()
    hive.createDatabase(db, ignoreIfExists)
  }

  override def dropDatabase(
      hive: Hive,
      dbName: String,
      deleteData: Boolean,
      ignoreUnknownDb: Boolean,
      cascade: Boolean): Unit = {
    recordHiveCall()
    hive.dropDatabase(dbName, deleteData, ignoreUnknownDb, cascade)
  }

  override def alterDatabase(hive: Hive, dbName: String, d: Database): Unit = {
    recordHiveCall()
    hive.alterDatabase(dbName, d)
  }

  override def getDatabase(hive: Hive, dbName: String): Database = {
    recordHiveCall()
    hive.getDatabase(dbName)
  }

  override def getAllDatabases(hive: Hive): Seq[String] = {
    recordHiveCall()
    hive.getAllDatabases.asScala.toSeq
  }

  override def getDatabasesByPattern(hive: Hive, pattern: String): Seq[String] = {
    recordHiveCall()
    hive.getDatabasesByPattern(pattern).asScala.toSeq
  }

  override def databaseExists(hive: Hive, dbName: String): Boolean = {
    recordHiveCall()
    hive.databaseExists(dbName)
  }

  override def createTable(hive: Hive, table: Table, ifNotExists: Boolean): Unit = {
    recordHiveCall()
    hive.createTable(table, ifNotExists)
  }

  override def getTable(
      hive: Hive,
      dbName: String,
      tableName: String,
      throwException: Boolean): Table = {
    recordHiveCall()
    val table = hive.getTable(dbName, tableName, throwException)
    if (table != null) {
      table.getTTable.setTableName(tableName)
      table.getTTable.setDbName(dbName)
    }
    table
  }

  override def getTablesByPattern(hive: Hive, dbName: String, pattern: String): Seq[String] = {
    recordHiveCall()
    hive.getTablesByPattern(dbName, pattern).asScala.toSeq
  }

  override def getAllTables(hive: Hive, dbName: String): Seq[String] = {
    recordHiveCall()
    hive.getAllTables(dbName).asScala.toSeq
  }

  override def dropTable(hive: Hive, dbName: String, tableName: String): Unit = {
    recordHiveCall()
    hive.dropTable(dbName, tableName)
  }

  override def getPartition(
      hive: Hive,
      table: Table,
      partSpec: JMap[String, String],
      forceCreate: Boolean): Partition = {
    recordHiveCall()
    hive.getPartition(table, partSpec, forceCreate)
  }

  override def getPartitions(
      hive: Hive,
      table: Table,
      partSpec: JMap[String, String]): Seq[Partition] = {
    recordHiveCall()
    hive.getPartitions(table, partSpec).asScala.toSeq
  }

  override def getPartitionNames(
      hive: Hive,
      dbName: String,
      tableName: String,
      max: Short): Seq[String] = {
    recordHiveCall()
    hive.getPartitionNames(dbName, tableName, max).asScala.toSeq
  }

  override def getPartitionNames(
      hive: Hive,
      dbName: String,
      tableName: String,
      partSpec: JMap[String, String],
      max: Short): Seq[String] = {
    recordHiveCall()
    hive.getPartitionNames(dbName, tableName, partSpec, max).asScala.toSeq
  }

  override def renamePartition(
      hive: Hive,
      table: Table,
      oldPartSpec: JMap[String, String],
      newPart: Partition): Unit = {
    recordHiveCall()
    hive.renamePartition(table, oldPartSpec, newPart)
  }

  override def getIndexes(
      hive: Hive,
      dbName: String,
      tableName: String,
      max: Short): Seq[Index] = {
    recordHiveCall()
    hive.getIndexes(dbName, tableName, max).asScala.toSeq
  }
}

private[client] class Shim_v2_1 extends Shim_v2_0 {

  // true if there is any following stats task
  protected lazy val hasFollowingStatsTask = JBoolean.FALSE
  // TODO: Now, always set environmentContext to null. In the future, we should avoid setting
  // hive-generated stats to -1 when altering tables by using environmentContext. See Hive-12730
  protected lazy val environmentContextInAlterTable = null

  private lazy val loadPartitionMethod =
    findMethod(
      classOf[Hive],
      "loadPartition",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadTableMethod =
    findMethod(
      classOf[Hive],
      "loadTable",
      classOf[Path],
      classOf[String],
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      JBoolean.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JLong.TYPE,
      JBoolean.TYPE,
      classOf[AcidUtils.Operation])
  private lazy val alterTableMethod =
    findMethod(
      classOf[Hive],
      "alterTable",
      classOf[String],
      classOf[Table],
      classOf[EnvironmentContext])
  private lazy val alterPartitionsMethod =
    findMethod(
      classOf[Hive],
      "alterPartitions",
      classOf[String],
      classOf[JList[Partition]],
      classOf[EnvironmentContext])

  override def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean,
      isSrcLocal: Boolean): Unit = {
    recordHiveCall()
    loadPartitionMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      inheritTableSpecs: JBoolean, isSkewedStoreAsSubdir: JBoolean,
      isSrcLocal: JBoolean, isAcid, hasFollowingStatsTask)
  }

  override def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit = {
    recordHiveCall()
    loadTableMethod.invoke(hive, loadPath, tableName, replace: JBoolean, isSrcLocal: JBoolean,
      isSkewedStoreAsSubdir, isAcid, hasFollowingStatsTask)
  }

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      hiveTable: Table): Unit = {
    recordHiveCall()
    val listBucketingEnabled = hiveTable.isStoredAsSubDirectories
    loadDynamicPartitionsMethod.invoke(hive, loadPath, tableName, partSpec, replace: JBoolean,
      numDP: JInteger, listBucketingEnabled: JBoolean, isAcid, txnIdInLoadDynamicPartitions,
      hasFollowingStatsTask, AcidUtils.Operation.NOT_ACID)
  }

  override def alterTable(hive: Hive, tableName: String, table: Table): Unit = {
    recordHiveCall()
    alterTableMethod.invoke(hive, tableName, table, environmentContextInAlterTable)
  }

  override def alterPartitions(hive: Hive, tableName: String, newParts: JList[Partition]): Unit = {
    recordHiveCall()
    alterPartitionsMethod.invoke(hive, tableName, newParts, environmentContextInAlterTable)
  }
}

private[client] class Shim_v2_2 extends Shim_v2_1

private[client] class Shim_v2_3 extends Shim_v2_1 {
  private lazy val getTablesByTypeMethod =
    findMethod(
      classOf[Hive],
      "getTablesByType",
      classOf[String],
      classOf[String],
      classOf[TableType])

  override def getTablesByType(
      hive: Hive,
      dbName: String,
      pattern: String,
      tableType: TableType): Seq[String] = {
    recordHiveCall()
    try {
      getTablesByTypeMethod.invoke(hive, dbName, pattern, tableType)
        .asInstanceOf[JList[String]].asScala.toSeq
    } catch {
      case ex: InvocationTargetException if ex.getCause.isInstanceOf[HiveException] =>
        val cause = ex.getCause.getCause
        if (cause != null && cause.isInstanceOf[MetaException] &&
          cause.getMessage != null &&
          cause.getMessage.contains("Invalid method name: 'get_tables_by_type'")) {
          // SparkUnsupportedOperationException (inherited from UnsupportedOperationException)
          // is thrown when the Shim_v2_3#getTablesByType call returns no get_tables_by_type method.
          // HiveClientImpl#listTablesByType will have fallback processing.
          throw QueryExecutionErrors.getTablesByTypeUnsupportedByHiveVersionError()
        } else {
          throw ex
        }
    }
  }
}

private[client] class Shim_v3_0 extends Shim_v2_3 {
  // Spark supports only non-ACID operations
  protected lazy val isAcidIUDoperation = JBoolean.FALSE

  // Writer ID can be 0 for non-ACID operations
  protected lazy val writeIdInLoadTableOrPartition: JLong = 0L

  // Statement ID
  protected lazy val stmtIdInLoadTableOrPartition: JInteger = 0

  protected lazy val listBucketingLevel: JInteger = 0

  private lazy val clazzLoadFileType = getClass.getClassLoader.loadClass(
    "org.apache.hadoop.hive.ql.plan.LoadTableDesc$LoadFileType")

  private lazy val loadPartitionMethod =
    findMethod(
      classOf[Hive],
      "loadPartition",
      classOf[Path],
      classOf[Table],
      classOf[JMap[String, String]],
      clazzLoadFileType,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      classOf[JLong],
      JInteger.TYPE,
      JBoolean.TYPE)
  private lazy val loadTableMethod =
    findMethod(
      classOf[Hive],
      "loadTable",
      classOf[Path],
      classOf[String],
      clazzLoadFileType,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      classOf[JLong],
      JInteger.TYPE,
      JBoolean.TYPE)
  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      classOf[Path],
      classOf[String],
      classOf[JMap[String, String]],
      clazzLoadFileType,
      JInteger.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      JLong.TYPE,
      JInteger.TYPE,
      JBoolean.TYPE,
      classOf[AcidUtils.Operation],
      JBoolean.TYPE)

  override def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean,
      isSrcLocal: Boolean): Unit = {
    recordHiveCall()
    val table = hive.getTable(tableName)
    val loadFileType = if (replace) {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("REPLACE_ALL"))
    } else {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("KEEP_EXISTING"))
    }
    assert(loadFileType.isDefined)
    recordHiveCall()
    loadPartitionMethod.invoke(hive, loadPath, table, partSpec, loadFileType.get,
      inheritTableSpecs: JBoolean, isSkewedStoreAsSubdir: JBoolean,
      isSrcLocal: JBoolean, isAcid, hasFollowingStatsTask,
      writeIdInLoadTableOrPartition, stmtIdInLoadTableOrPartition, replace: JBoolean)
  }

  override def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit = {
    val loadFileType = if (replace) {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("REPLACE_ALL"))
    } else {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("KEEP_EXISTING"))
    }
    assert(loadFileType.isDefined)
    recordHiveCall()
    loadTableMethod.invoke(hive, loadPath, tableName, loadFileType.get, isSrcLocal: JBoolean,
      isSkewedStoreAsSubdir, isAcidIUDoperation, hasFollowingStatsTask,
      writeIdInLoadTableOrPartition, stmtIdInLoadTableOrPartition: JInteger, replace: JBoolean)
  }

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      hiveTable: Table): Unit = {
    val loadFileType = if (replace) {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("REPLACE_ALL"))
    } else {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("KEEP_EXISTING"))
    }
    assert(loadFileType.isDefined)
    recordHiveCall()
    loadDynamicPartitionsMethod.invoke(hive, loadPath, tableName, partSpec, loadFileType.get,
      numDP: JInteger, listBucketingLevel, isAcid, writeIdInLoadTableOrPartition,
      stmtIdInLoadTableOrPartition, hasFollowingStatsTask, AcidUtils.Operation.NOT_ACID,
      replace: JBoolean)
  }
}

private[client] class Shim_v3_1 extends Shim_v3_0

private[client] class Shim_v4_0 extends Shim_v3_1 {
  private lazy val clazzLoadFileType = getClass.getClassLoader.loadClass(
    "org.apache.hadoop.hive.ql.plan.LoadTableDesc$LoadFileType")
  private lazy val clazzLoadTableDesc = getClass.getClassLoader.loadClass(
    "org.apache.hadoop.hive.ql.plan.LoadTableDesc")
  private lazy val clazzPartitionDetails =
    Utils.classForName("org.apache.hadoop.hive.ql.exec.Utilities$PartitionDesc")

  override protected lazy val wildcard: String = "%"

  private lazy val alterTableMethod =
    findMethod(
      classOf[Hive],
      "alterTable",
      classOf[String],
      classOf[Table],
      classOf[EnvironmentContext],
      JBoolean.TYPE)
  private lazy val loadTableMethod =
    findMethod(
      classOf[Hive],
      "loadTable",
      classOf[Path],
      classOf[String],
      clazzLoadFileType,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      classOf[JLong],
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val addPartitionsMethod =
    findMethod(
      classOf[Hive],
      "addPartitions",
      classOf[JList[Partition]],
      JBoolean.TYPE,
      JBoolean.TYPE) // needResults
  private lazy val alterPartitionsMethod =
    findMethod(
      classOf[Hive],
      "alterPartitions",
      classOf[String],
      classOf[JList[Partition]],
      classOf[EnvironmentContext],
      JBoolean.TYPE)
  private lazy val loadPartitionMethod =
    findMethod(
      classOf[Hive],
      "loadPartition",
      classOf[Path],
      classOf[Table],
      classOf[JMap[String, String]],
      clazzLoadFileType,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE,
      classOf[JLong],
      JInteger.TYPE,
      JBoolean.TYPE,
      JBoolean.TYPE)
  private lazy val loadDynamicPartitionsMethod =
    findMethod(
      classOf[Hive],
      "loadDynamicPartitions",
      clazzLoadTableDesc,
      JInteger.TYPE, // numLB number of buckets
      JBoolean.TYPE, // isAcid true if this is an ACID operation
      JLong.TYPE, // writeId, can be 0 unless isAcid == true
      JInteger.TYPE, // stmtId
      JBoolean.TYPE, // resetStatistics
      classOf[AcidUtils.Operation],
      classOf[JMap[Path, clazzPartitionDetails.type]])
  private lazy val renamePartitionMethod =
    findMethod(
      classOf[Hive],
      "renamePartition",
      classOf[Table],
      classOf[JMap[String, String]],
      classOf[Partition],
      JLong.TYPE)

  override def alterTable(hive: Hive, tableName: String, table: Table): Unit = {
    recordHiveCall()
    val transactional = false
    alterTableMethod.invoke(
      hive,
      tableName,
      table,
      environmentContextInAlterTable,
      transactional: JBoolean
    )
  }

  override def loadTable(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      replace: Boolean,
      isSrcLocal: Boolean): Unit = {
    val loadFileType = if (replace) {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("REPLACE_ALL"))
    } else {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("KEEP_EXISTING"))
    }
    assert(loadFileType.isDefined)
    recordHiveCall()
    val resetStatistics = false
    val isDirectInsert = false
    loadTableMethod.invoke(
      hive,
      loadPath,
      tableName,
      loadFileType.get,
      isSrcLocal: JBoolean,
      isSkewedStoreAsSubdir,
      isAcidIUDoperation,
      resetStatistics,
      writeIdInLoadTableOrPartition,
      stmtIdInLoadTableOrPartition: JInteger,
      replace: JBoolean,
      isDirectInsert: JBoolean)
  }

  override def alterPartitions(
      hive: Hive,
      tableName: String,
      newParts: JList[Partition]): Unit = {
    recordHiveCall()
    val transactional: JBoolean = false
    alterPartitionsMethod.invoke(
      hive,
      tableName,
      newParts,
      environmentContextInAlterTable,
      transactional)
  }

  override def createPartitions(
      hive: Hive,
      table: Table,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val partitions = parts.map(HiveClientImpl.toHivePartition(_, table).getTPartition).asJava
    recordHiveCall()
    val needResults = false
    addPartitionsMethod.invoke(hive, partitions, ignoreIfExists, needResults: JBoolean)
  }

  override def loadPartition(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      inheritTableSpecs: Boolean,
      isSkewedStoreAsSubdir: Boolean,
      isSrcLocal: Boolean): Unit = {
    recordHiveCall()
    val table = hive.getTable(tableName)
    val loadFileType = if (replace) {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("REPLACE_ALL"))
    } else {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("KEEP_EXISTING"))
    }
    assert(loadFileType.isDefined)
    val inheritLocation: JBoolean = false
    val isDirectInsert: JBoolean = false
    recordHiveCall()
    loadPartitionMethod.invoke(
      hive,
      loadPath,
      table,
      partSpec,
      loadFileType.get,
      inheritTableSpecs: JBoolean,
      inheritLocation,
      isSkewedStoreAsSubdir: JBoolean,
      isSrcLocal: JBoolean,
      isAcid,
      hasFollowingStatsTask,
      writeIdInLoadTableOrPartition,
      stmtIdInLoadTableOrPartition,
      replace: JBoolean,
      isDirectInsert
    )
  }

  override def loadDynamicPartitions(
      hive: Hive,
      loadPath: Path,
      tableName: String,
      partSpec: JMap[String, String],
      replace: Boolean,
      numDP: Int,
      hiveTable: Table): Unit = {
    import org.apache.hadoop.hive.ql.exec.Utilities
    val loadFileType = if (replace) {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("REPLACE_ALL"))
    } else {
      clazzLoadFileType.getEnumConstants.find(_.toString.equalsIgnoreCase("KEEP_EXISTING"))
    }
    assert(loadFileType.isDefined)

    val useAppendForLoad: JBoolean = false
    val loadTableDesc = clazzLoadTableDesc
      .getConstructor(
        classOf[Path],
        classOf[Table],
        JBoolean.TYPE,
        JBoolean.TYPE,
        classOf[JMap[String, String]])
      .newInstance(
        loadPath,
        hiveTable,
        replace: JBoolean,
        useAppendForLoad,
        partSpec)
    val ctx = new DynamicPartitionCtx()
    ctx.setRootPath(loadPath)
    ctx.setNumDPCols(numDP)
    ctx.setPartSpec(partSpec)

    val fullDPSpecs = classOf[Utilities].getMethod(
      "getFullDPSpecs",
      classOf[Configuration],
      classOf[DynamicPartitionCtx],
      classOf[java.util.Map[String, java.util.List[Path]]])
    recordHiveCall()
    val resetPartitionStats: JBoolean = false
    loadDynamicPartitionsMethod.invoke(
      hive,
      loadTableDesc,
      listBucketingLevel,
      isAcid,
      writeIdInLoadTableOrPartition,
      stmtIdInLoadTableOrPartition,
      resetPartitionStats,
      AcidUtils.Operation.NOT_ACID,
      fullDPSpecs.invoke(null, hive.getConf, ctx, null)
    )
  }

  override def renamePartition(
      hive: Hive,
      table: Table,
      oldPartSpec: JMap[String, String],
      newPart: Partition): Unit = {
    recordHiveCall()
    renamePartitionMethod.invoke(hive, table, oldPartSpec, newPart, writeIdInLoadTableOrPartition)
  }
}
