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

package org.apache.spark.sql.connector.catalog;

import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

/**
 * An optional mix-in for implementations of {@link TableCatalog} that support staging creation of
 * the a table before committing the table's metadata along with its contents in CREATE TABLE AS
 * SELECT or REPLACE TABLE AS SELECT operations.
 * <p>
 * It is highly recommended to implement this trait whenever possible so that CREATE TABLE AS
 * SELECT and REPLACE TABLE AS SELECT operations are atomic. For example, when one runs a REPLACE
 * TABLE AS SELECT operation, if the catalog does not implement this trait, the planner will first
 * drop the table via {@link TableCatalog#dropTable(Identifier)}, then create the table via
 * {@link TableCatalog#createTable(Identifier, StructType, Transform[], Map)}, and then perform
 * the write via {@link SupportsWrite#newWriteBuilder(LogicalWriteInfo)}.
 * However, if the write operation fails, the catalog will have already dropped the table, and the
 * planner cannot roll back the dropping of the table.
 * <p>
 * If the catalog implements this plugin, the catalog can implement the methods to "stage" the
 * creation and the replacement of a table. After the table's
 * {@link BatchWrite#commit(WriterCommitMessage[])} is called,
 * {@link StagedTable#commitStagedChanges()} is called, at which point the staged table can
 * complete both the data write and the metadata swap operation atomically.
 *
 * @since 3.0.0
 */
@Evolving
public interface StagingTableCatalog extends TableCatalog {

  /**
   * Stage the creation of a table, preparing it to be committed into the metastore.
   * <p>
   * This is deprecated. Please override
   * {@link #stageCreate(Identifier, Column[], Transform[], Map)} instead.
   */
  @Deprecated
  StagedTable stageCreate(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException;

  /**
   * Stage the creation of a table, preparing it to be committed into the metastore.
   * <p>
   * When the table is committed, the contents of any writes performed by the Spark planner are
   * committed along with the metadata about the table passed into this method's arguments. If the
   * table exists when this method is called, the method should throw an exception accordingly. If
   * another process concurrently creates the table before this table's staged changes are
   * committed, an exception should be thrown by {@link StagedTable#commitStagedChanges()}.
   *
   * @param ident a table identifier
   * @param columns the column of the new table
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table. This can be null if the catalog does not support atomic
   *         creation for this table. Spark will call {@link #loadTable(Identifier)} later.
   * @throws TableAlreadyExistsException If a table or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  default StagedTable stageCreate(
      Identifier ident,
      Column[] columns,
      Transform[] partitions,
      Map<String, String> properties) throws TableAlreadyExistsException, NoSuchNamespaceException {
    return stageCreate(ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties);
  }

  /**
   * Stage the replacement of a table, preparing it to be committed into the metastore when the
   * returned table's {@link StagedTable#commitStagedChanges()} is called.
   * <p>
   * This is deprecated, please override
   * {@link #stageReplace(Identifier, StructType, Transform[], Map)} instead.
   */
  StagedTable stageReplace(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws NoSuchNamespaceException, NoSuchTableException;

  /**
   * Stage the replacement of a table, preparing it to be committed into the metastore when the
   * returned table's {@link StagedTable#commitStagedChanges()} is called.
   * <p>
   * When the table is committed, the contents of any writes performed by the Spark planner are
   * committed along with the metadata about the table passed into this method's arguments. If the
   * table exists, the metadata and the contents of this table replace the metadata and contents of
   * the existing table. If a concurrent process commits changes to the table's data or metadata
   * while the write is being performed but before the staged changes are committed, the catalog
   * can decide whether to move forward with the table replacement anyways or abort the commit
   * operation.
   * <p>
   * If the table does not exist, committing the staged changes should fail with
   * {@link NoSuchTableException}. This differs from the semantics of
   * {@link #stageCreateOrReplace(Identifier, StructType, Transform[], Map)}, which should create
   * the table in the data source if the table does not exist at the time of committing the
   * operation.
   *
   * @param ident a table identifier
   * @param columns the columns of the new table
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table. This can be null if the catalog does not support atomic
   *         creation for this table. Spark will call {@link #loadTable(Identifier)} later.
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   * @throws NoSuchTableException If the table does not exist
   */
  default StagedTable stageReplace(
      Identifier ident,
      Column[] columns,
      Transform[] partitions,
      Map<String, String> properties) throws NoSuchNamespaceException, NoSuchTableException {
    return stageReplace(
      ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties);
  }

  /**
   * Stage the creation or replacement of a table, preparing it to be committed into the metastore
   * when the returned table's {@link StagedTable#commitStagedChanges()} is called.
   * <p>
   * This is deprecated, please override
   * {@link #stageCreateOrReplace(Identifier, Column[], Transform[], Map)} instead.
   */
  StagedTable stageCreateOrReplace(
      Identifier ident,
      StructType schema,
      Transform[] partitions,
      Map<String, String> properties) throws NoSuchNamespaceException;

  /**
   * Stage the creation or replacement of a table, preparing it to be committed into the metastore
   * when the returned table's {@link StagedTable#commitStagedChanges()} is called.
   * <p>
   * When the table is committed, the contents of any writes performed by the Spark planner are
   * committed along with the metadata about the table passed into this method's arguments. If the
   * table exists, the metadata and the contents of this table replace the metadata and contents of
   * the existing table. If a concurrent process commits changes to the table's data or metadata
   * while the write is being performed but before the staged changes are committed, the catalog
   * can decide whether to move forward with the table replacement anyways or abort the commit
   * operation.
   * <p>
   * If the table does not exist when the changes are committed, the table should be created in the
   * backing data source. This differs from the expected semantics of
   * {@link #stageReplace(Identifier, StructType, Transform[], Map)}, which should fail when
   * the staged changes are committed but the table doesn't exist at commit time.
   *
   * @param ident a table identifier
   * @param columns the columns of the new table
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table properties
   * @return metadata for the new table. This can be null if the catalog does not support atomic
   *         creation for this table. Spark will call {@link #loadTable(Identifier)} later.
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  default StagedTable stageCreateOrReplace(
      Identifier ident,
      Column[] columns,
      Transform[] partitions,
      Map<String, String> properties) throws NoSuchNamespaceException {
    return stageCreateOrReplace(
      ident, CatalogV2Util.v2ColumnsToStructType(columns), partitions, properties);
  }
}
