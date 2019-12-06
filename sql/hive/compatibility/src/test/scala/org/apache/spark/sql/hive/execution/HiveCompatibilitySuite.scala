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

import java.io.File
import java.util.{Locale, TimeZone}

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy

/**
 * Runs the test cases that are included in the hive distribution.
 */
class HiveCompatibilitySuite extends HiveQueryFileTest with BeforeAndAfter {
  // TODO: bundle in jar files... get from classpath
  private lazy val hiveQueryDir = TestHive.getHiveFile(
    "ql/src/test/queries/clientpositive".split("/").mkString(File.separator))

  private val originalTimeZone = TimeZone.getDefault
  private val originalLocale = Locale.getDefault
  private val originalColumnBatchSize = TestHive.conf.columnBatchSize
  private val originalInMemoryPartitionPruning = TestHive.conf.inMemoryPartitionPruning
  private val originalCrossJoinEnabled = TestHive.conf.crossJoinEnabled
  private val originalSessionLocalTimeZone = TestHive.conf.sessionLocalTimeZone

  def testCases: Seq[(String, File)] = {
    hiveQueryDir.listFiles.map(f => f.getName.stripSuffix(".q") -> f)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHive.setCacheTables(true)
    // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
    TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
    // Add Locale setting
    Locale.setDefault(Locale.US)
    // Set a relatively small column batch size for testing purposes
    TestHive.setConf(SQLConf.COLUMN_BATCH_SIZE, 5)
    // Enable in-memory partition pruning for testing purposes
    TestHive.setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, true)
    // Ensures that cross joins are enabled so that we can test them
    TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, true)
    // Ensures that the table insertion behaivor is consistent with Hive
    TestHive.setConf(SQLConf.STORE_ASSIGNMENT_POLICY, StoreAssignmentPolicy.LEGACY.toString)
    // Fix session local timezone to America/Los_Angeles for those timezone sensitive tests
    // (timestamp_*)
    TestHive.setConf(SQLConf.SESSION_LOCAL_TIMEZONE, "America/Los_Angeles")
    RuleExecutor.resetMetrics()
  }

  override def afterAll(): Unit = {
    try {
      TestHive.setCacheTables(false)
      TimeZone.setDefault(originalTimeZone)
      Locale.setDefault(originalLocale)
      TestHive.setConf(SQLConf.COLUMN_BATCH_SIZE, originalColumnBatchSize)
      TestHive.setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, originalInMemoryPartitionPruning)
      TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, originalCrossJoinEnabled)
      TestHive.setConf(SQLConf.SESSION_LOCAL_TIMEZONE, originalSessionLocalTimeZone)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
    } finally {
      super.afterAll()
    }
  }

  /** A list of tests deemed out of scope currently and thus completely disregarded. */
  override def blackList: Seq[String] = Seq(
    // These tests use hooks that are not on the classpath and thus break all subsequent execution.
    "hook_order",
    "hook_context_cs",
    "mapjoin_hook",
    "multi_sahooks",
    "overridden_confs",
    "query_properties",
    "sample10",
    "updateAccessTime",
    "index_compact_binary_search",
    "bucket_num_reducers",
    "column_access_stats",
    "concatenate_inherit_table_location",
    "describe_pretty",
    "describe_syntax",
    "orc_ends_with_nulls",

    // Setting a default property does not seem to get reset and thus changes the answer for many
    // subsequent tests.
    "create_default_prop",

    // User/machine specific test answers, breaks the caching mechanism.
    "authorization_3",
    "authorization_5",
    "keyword_1",
    "misc_json",
    "load_overwrite",
    "alter_table_serde2",
    "alter_table_not_sorted",
    "alter_skewed_table",
    "alter_partition_clusterby_sortby",
    "alter_merge",
    "alter_concatenate_indexed_table",
    "protectmode2",
    // "describe_table",
    "describe_comment_nonascii",

    "create_merge_compressed",
    "create_view",
    "create_view_partitioned",
    "database_location",
    "database_properties",

    // DFS commands
    "symlink_text_input_format",

    // Weird DDL differences result in failures on jenkins.
    "create_like2",
    "partitions_json",

    // This test is totally fine except that it includes wrong queries and expects errors, but error
    // message format in Hive and Spark SQL differ. Should workaround this later.
    "udf_to_unix_timestamp",
    // we can cast dates likes '2015-03-18' to a timestamp and extract the seconds.
    // Hive returns null for second('2015-03-18')
    "udf_second",
    // we can cast dates likes '2015-03-18' to a timestamp and extract the minutes.
    // Hive returns null for minute('2015-03-18')
    "udf_minute",


    // Cant run without local map/reduce.
    "index_auto_update",
    "index_auto_self_join",
    "index_stale.*",
    "index_compression",
    "index_bitmap_compression",
    "index_auto_multiple",
    "index_auto_mult_tables_compact",
    "index_auto_mult_tables",
    "index_auto_file_format",
    "index_auth",
    "index_auto_empty",
    "index_auto_partitioned",
    "index_auto_unused",
    "index_bitmap_auto_partitioned",
    "ql_rewrite_gbtoidx",
    "stats1.*",
    "stats20",
    "alter_merge_stats",
    "columnstats.*",
    "annotate_stats.*",
    "database_drop",
    "index_serde",


    // Hive seems to think 1.0 > NaN = true && 1.0 < NaN = false... which is wrong.
    // http://stackoverflow.com/a/1573715
    "ops_comparison",

    // Tests that seems to never complete on hive...
    "skewjoin",
    "database",

    // These tests fail and exit the JVM.
    "auto_join18_multi_distinct",
    "join18_multi_distinct",
    "input44",
    "input42",
    "input_dfs",
    "metadata_export_drop",
    "repair",

    // Uses a serde that isn't on the classpath... breaks other tests.
    "bucketizedhiveinputformat",

    // Avro tests seem to change the output format permanently thus breaking the answer cache, until
    // we figure out why this is the case let just ignore all of avro related tests.
    ".*avro.*",

    // Unique joins are weird and will require a lot of hacks (see comments in hive parser).
    "uniquejoin",

    // Hive seems to get the wrong answer on some outer joins.  MySQL agrees with catalyst.
    "auto_join29",

    // No support for multi-alias i.e. udf as (e1, e2, e3).
    "allcolref_in_udf",

    // No support for TestSerDe (not published afaik)
    "alter1",
    "input16",

    // No support for unpublished test udfs.
    "autogen_colalias",

    // Hive does not support buckets.
    ".*bucket.*",

    // We have our own tests based on these query files.
    ".*window.*",

    // Fails in hive with authorization errors.
    "alter_rename_partition_authorization",
    "authorization.*",

    // Hadoop version specific tests
    "archive_corrupt",

    // No support for case sensitivity is resolution using hive properties atm.
    "case_sensitivity",

    // Flaky test, Hive sometimes returns different set of 10 rows.
    "lateral_view_outer",

    // After stop taking the `stringOrError` route, exceptions are thrown from these cases.
    // See SPARK-2129 for details.
    "join_view",
    "mergejoins_mixed",

    // Returning the result of a describe state as a JSON object is not supported.
    "describe_table_json",
    "describe_database_json",
    "describe_formatted_view_partitioned_json",

    // Hive returns the results of describe as plain text. Comments with multiple lines
    // introduce extra lines in the Hive results, which make the result comparison fail.
    "describe_comment_indent",

    // Limit clause without a ordering, which causes failure.
    "orc_predicate_pushdown",

    // Requires precision decimal support:
    "udf_when",
    "udf_case",

    // the table src(key INT, value STRING) is not the same as HIVE unittest. In Hive
    // is src(key STRING, value STRING), and in the reflect.q, it failed in
    // Integer.valueOf, which expect the first argument passed as STRING type not INT.
    "udf_reflect",

    // Sort with Limit clause causes failure.
    "ctas",
    "ctas_hadoop20",

    // timestamp in array, the output format of Hive contains double quotes, while
    // Spark SQL doesn't
    "udf_sort_array",

    // It has a bug and it has been fixed by
    // https://issues.apache.org/jira/browse/HIVE-7673 (in Hive 0.14 and trunk).
    "input46",

    // These tests were broken by the hive client isolation PR.
    "part_inherit_tbl_props",
    "part_inherit_tbl_props_with_star",

    "nullformatCTAS", // SPARK-7411: need to finish CTAS parser

    // The isolated classloader seemed to make some of our test reset mechanisms less robust.
    "combine1", // This test changes compression settings in a way that breaks all subsequent tests.
    "load_dyn_part14.*", // These work alone but fail when run with other tests...

    // the answer is sensitive for jdk version
    "udf_java_method",

    // Spark SQL use Long for TimestampType, lose the precision under 1us
    "timestamp_1",
    "timestamp_2",
    "timestamp_udf",

    // Hive returns string from UTC formatted timestamp, spark returns timestamp type
    "date_udf",

    // Can't compare the result that have newline in it
    "udf_get_json_object",

    // Unlike Hive, we do support log base in (0, 1.0], therefore disable this
    "udf7",

    // Trivial changes to DDL output
    "compute_stats_empty_table",
    "compute_stats_long",
    "create_view_translate",
    "show_tblproperties",

    // Odd changes to output
    "merge4",

    // Unsupported underscore syntax.
    "inputddl5",

    // Thift is broken...
    "inputddl8",

    // Hive changed ordering of ddl:
    "varchar_union1",

    // Parser changes in Hive 1.2
    "input25",
    "input26",

    // Uses invalid table name
    "innerjoin",

    // classpath problems
    "compute_stats.*",
    "udf_bitmap_.*",

    // The difference between the double numbers generated by Hive and Spark
    // can be ignored (e.g., 0.6633880657639323 and 0.6633880657639322)
    "udaf_corr",

    // Feature removed in HIVE-11145
    "alter_partition_protect_mode",
    "drop_partitions_ignore_protection",
    "protectmode",

    // Hive returns null rather than NaN when n = 1
    "udaf_covar_samp",

    // The implementation of GROUPING__ID in Hive is wrong (not match with doc).
    "groupby_grouping_id1",
    "groupby_grouping_id2",
    "groupby_grouping_sets1",

    // Spark parser treats numerical literals differently: it creates decimals instead of doubles.
    "udf_abs",
    "udf_format_number",
    "udf_round",
    "udf_round_3",
    "view_cast",

    // These tests check the VIEW table definition, but Spark handles CREATE VIEW itself and
    // generates different View Expanded Text.
    "alter_view_as_select",

    // We don't support show create table commands in general
    "show_create_table_alter",
    "show_create_table_db_table",
    "show_create_table_delimited",
    "show_create_table_does_not_exist",
    "show_create_table_index",
    "show_create_table_partitioned",
    "show_create_table_serde",
    "show_create_table_view",

    // These tests try to change how a table is bucketed, which we don't support
    "alter4",
    "sort_merge_join_desc_5",
    "sort_merge_join_desc_6",
    "sort_merge_join_desc_7",

    // These tests try to create a table with bucketed columns, which we don't support
    "auto_join32",
    "auto_join_filters",
    "auto_smb_mapjoin_14",
    "ct_case_insensitive",
    "explain_rearrange",
    "groupby_sort_10",
    "groupby_sort_2",
    "groupby_sort_3",
    "groupby_sort_4",
    "groupby_sort_5",
    "groupby_sort_7",
    "groupby_sort_8",
    "groupby_sort_9",
    "groupby_sort_test_1",
    "inputddl4",
    "join_filters",
    "join_nulls",
    "join_nullsafe",
    "load_dyn_part2",
    "orc_empty_files",
    "reduce_deduplicate",
    "smb_mapjoin9",
    "smb_mapjoin_1",
    "smb_mapjoin_10",
    "smb_mapjoin_13",
    "smb_mapjoin_14",
    "smb_mapjoin_15",
    "smb_mapjoin_16",
    "smb_mapjoin_17",
    "smb_mapjoin_2",
    "smb_mapjoin_21",
    "smb_mapjoin_25",
    "smb_mapjoin_3",
    "smb_mapjoin_4",
    "smb_mapjoin_5",
    "smb_mapjoin_6",
    "smb_mapjoin_7",
    "smb_mapjoin_8",
    "sort_merge_join_desc_1",
    "sort_merge_join_desc_2",
    "sort_merge_join_desc_3",
    "sort_merge_join_desc_4",

    // These tests try to create a table with skewed columns, which we don't support
    "create_skewed_table1",
    "skewjoinopt13",
    "skewjoinopt18",
    "skewjoinopt9",

    // This test tries to create a table like with TBLPROPERTIES clause, which we don't support.
    "create_like_tbl_props",

    // Index commands are not supported
    "drop_index",
    "drop_index_removes_partition_dirs",
    "alter_index",
    "auto_sortmerge_join_1",
    "auto_sortmerge_join_10",
    "auto_sortmerge_join_11",
    "auto_sortmerge_join_12",
    "auto_sortmerge_join_13",
    "auto_sortmerge_join_14",
    "auto_sortmerge_join_15",
    "auto_sortmerge_join_16",
    "auto_sortmerge_join_2",
    "auto_sortmerge_join_3",
    "auto_sortmerge_join_4",
    "auto_sortmerge_join_5",
    "auto_sortmerge_join_6",
    "auto_sortmerge_join_7",
    "auto_sortmerge_join_8",
    "auto_sortmerge_join_9",

    // Macro commands are not supported
    "macro",

    // Create partitioned view is not supported
    "create_like_view",
    "describe_formatted_view_partitioned",

    // This uses CONCATENATE, which we don't support
    "alter_merge_2",

    // TOUCH is not supported
    "touch",

    // INPUTDRIVER and OUTPUTDRIVER are not supported
    "inoutdriver",

    // We do not support ALTER TABLE ADD COLUMN, ALTER TABLE REPLACE COLUMN,
    // ALTER TABLE CHANGE COLUMN, and ALTER TABLE SET FILEFORMAT.
    // We have converted the useful parts of these tests to tests
    // in org.apache.spark.sql.hive.execution.SQLQuerySuite.
    "alter_partition_format_loc",
    "alter_varchar1",
    "alter_varchar2",
    "date_3",
    "diff_part_input_formats",
    "disallow_incompatible_type_change_off",
    "fileformat_mix",
    "input3",
    "partition_schema1",
    "partition_wise_fileformat4",
    "partition_wise_fileformat5",
    "partition_wise_fileformat6",
    "partition_wise_fileformat7",
    "rename_column",

    // The following fails due to describe extended.
    "alter3",
    "alter5",
    "alter_table_serde",
    "input_part10",
    "input_part10_win",
    "inputddl6",
    "inputddl7",
    "part_inherit_tbl_props_empty",
    "serde_reported_schema",
    "stats0",
    "stats_empty_partition",
    "unicode_notation",
    "union_remove_11",
    "union_remove_3",

    // The following fails due to alter table partitions with predicate.
    "drop_partitions_filter",
    "drop_partitions_filter2",
    "drop_partitions_filter3",

    // The following failes due to truncate table
    "truncate_table",

    // We do not support DFS command.
    // We have converted the useful parts of these tests to tests
    // in org.apache.spark.sql.hive.execution.SQLQuerySuite.
    "drop_database_removes_partition_dirs",
    "drop_table_removes_partition_dirs",

    // These tests use EXPLAIN FORMATTED, which is not supported
    "input4",
    "join0",
    "plan_json",

    // This test uses CREATE EXTERNAL TABLE without specifying LOCATION
    "alter2",

    // [SPARK-16248][SQL] Whitelist the list of Hive fallback functions
    "udf_field",
    "udf_reflect2",
    "udf_xpath",
    "udf_xpath_boolean",
    "udf_xpath_double",
    "udf_xpath_float",
    "udf_xpath_int",
    "udf_xpath_long",
    "udf_xpath_short",
    "udf_xpath_string",

    // These tests DROP TABLE that don't exist (but do not specify IF EXISTS)
    "alter_rename_partition1",
    "date_1",
    "date_4",
    "date_join1",
    "date_serde",
    "insert_compressed",
    "lateral_view_cp",
    "leftsemijoin",
    "mapjoin_subquery2",
    "nomore_ambiguous_table_col",
    "partition_date",
    "partition_varchar1",
    "ppd_repeated_alias",
    "push_or",
    "reducesink_dedup",
    "subquery_in",
    "subquery_notin_having",
    "timestamp_3",
    "timestamp_lazy",
    "udaf_covar_pop",
    "union31",
    "union_date",
    "varchar_2",
    "varchar_join1",

    // This test assumes we parse scientific decimals as doubles (we parse them as decimals)
    "literal_double",

    // These tests are duplicates of joinXYZ
    "auto_join0",
    "auto_join1",
    "auto_join10",
    "auto_join11",
    "auto_join12",
    "auto_join13",
    "auto_join14",
    "auto_join14_hadoop20",
    "auto_join15",
    "auto_join17",
    "auto_join18",
    "auto_join2",
    "auto_join20",
    "auto_join21",
    "auto_join23",
    "auto_join24",
    "auto_join3",
    "auto_join4",
    "auto_join5",
    "auto_join6",
    "auto_join7",
    "auto_join8",
    "auto_join9",

    // These tests are based on the Hive's hash function, which is different from Spark
    "auto_join19",
    "auto_join22",
    "auto_join25",
    "auto_join26",
    "auto_join27",
    "auto_join28",
    "auto_join30",
    "auto_join31",
    "auto_join_nulls",
    "auto_join_reordering_values",
    "correlationoptimizer1",
    "correlationoptimizer2",
    "correlationoptimizer3",
    "correlationoptimizer4",
    "multiMapJoin1",
    "orc_dictionary_threshold",
    "udf_hash",
    // Moved to HiveQuerySuite
    "udf_radians"
  )

  private def commonWhiteList = Seq(
    "show_functions"
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not on whiteList or
   * blacklist are implicitly marked as ignored.
   */
  override def whiteList: Seq[String] = if (HiveUtils.isHive23) {
    commonWhiteList ++ Seq(
      "decimal_1_1"
    )
  } else {
    commonWhiteList
  }
}
