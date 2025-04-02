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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.tags.SlowHiveTest
import org.apache.spark.util.ArrayImplicits._

/**
 * Runs the test cases that are included in the hive distribution.
 */
@SlowHiveTest
class HiveCompatibilitySuite extends HiveQueryFileTest with BeforeAndAfter {
  // TODO: bundle in jar files... get from classpath
  private lazy val hiveQueryDir = TestHive.getHiveFile(
    "ql/src/test/queries/clientpositive".split("/").mkString(File.separator))

  private val originalColumnBatchSize = TestHive.conf.columnBatchSize
  private val originalInMemoryPartitionPruning = TestHive.conf.inMemoryPartitionPruning
  private val originalCrossJoinEnabled = TestHive.conf.crossJoinEnabled
  private val originalSessionLocalTimeZone = TestHive.conf.sessionLocalTimeZone
  private val originalAnsiMode = TestHive.conf.getConf(SQLConf.ANSI_ENABLED)
  private val originalStoreAssignmentPolicy =
    TestHive.conf.getConf(SQLConf.STORE_ASSIGNMENT_POLICY)
  private val originalCreateHiveTable =
    TestHive.conf.getConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT)

  def testCases: Seq[(String, File)] = {
    hiveQueryDir.listFiles.map(f => f.getName.stripSuffix(".q") -> f).toImmutableArraySeq
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    TestHive.setCacheTables(true)
    // Set a relatively small column batch size for testing purposes
    TestHive.setConf(SQLConf.COLUMN_BATCH_SIZE, 5)
    // Enable in-memory partition pruning for testing purposes
    TestHive.setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, true)
    // Ensures that cross joins are enabled so that we can test them
    TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, true)
    // Hive doesn't follow ANSI Standard.
    TestHive.setConf(SQLConf.ANSI_ENABLED, false)
    // Ensures that the table insertion behavior is consistent with Hive
    TestHive.setConf(SQLConf.STORE_ASSIGNMENT_POLICY, StoreAssignmentPolicy.LEGACY.toString)
    // Fix session local timezone to America/Los_Angeles for those timezone sensitive tests
    // (timestamp_*)
    TestHive.setConf(SQLConf.SESSION_LOCAL_TIMEZONE, "America/Los_Angeles")
    TestHive.setConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT, true)
    RuleExecutor.resetMetrics()
  }

  override def afterAll(): Unit = {
    try {
      TestHive.setCacheTables(false)
      TestHive.setConf(SQLConf.COLUMN_BATCH_SIZE, originalColumnBatchSize)
      TestHive.setConf(SQLConf.IN_MEMORY_PARTITION_PRUNING, originalInMemoryPartitionPruning)
      TestHive.setConf(SQLConf.CROSS_JOINS_ENABLED, originalCrossJoinEnabled)
      TestHive.setConf(SQLConf.SESSION_LOCAL_TIMEZONE, originalSessionLocalTimeZone)
      TestHive.setConf(SQLConf.ANSI_ENABLED, originalAnsiMode)
      TestHive.setConf(SQLConf.STORE_ASSIGNMENT_POLICY, originalStoreAssignmentPolicy)
      TestHive.setConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT, originalCreateHiveTable)

      // For debugging dump some statistics about how much time was spent in various optimizer rules
      logWarning(RuleExecutor.dumpTimeSpent())
    } finally {
      super.afterAll()
    }
  }

  /** A list of tests deemed out of scope currently and thus completely disregarded. */
  override def excludeList: Seq[String] = Seq(
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

    // Hive DESC FUNCTION returns a string to indicate undefined function, while Spark triggers a
    // query error.
    "udf_index",
    "udf_stddev_pop",

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

    // Thrift is broken...
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

    // The following fails due to truncate table
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

    // [SPARK-16248][SQL] Include the list of Hive fallback functions
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

  private def commonIncludeList = Seq(
    "add_part_exist",
    "add_part_multiple",
    "add_partition_no_whitelist",
    "add_partition_with_whitelist",
    "alias_casted_column",
    "alter_partition_with_whitelist",
    "ambiguous_col",
    "annotate_stats_join",
    "annotate_stats_limit",
    "annotate_stats_part",
    "annotate_stats_table",
    "annotate_stats_union",
    "binary_constant",
    "binarysortable_1",
    "cast1",
    "cluster",
    "combine1",
    "compute_stats_binary",
    "compute_stats_boolean",
    "compute_stats_double",
    "compute_stats_empty_table",
    "compute_stats_long",
    "compute_stats_string",
    "convert_enum_to_string",
    "correlationoptimizer10",
    "correlationoptimizer11",
    "correlationoptimizer13",
    "correlationoptimizer14",
    "correlationoptimizer15",
    "correlationoptimizer6",
    "correlationoptimizer7",
    "correlationoptimizer8",
    "correlationoptimizer9",
    "count",
    "cp_mj_rc",
    "create_insert_outputformat",
    "create_nested_type",
    "create_struct_table",
    "create_view_translate",
    "cross_join",
    "cross_product_check_1",
    "cross_product_check_2",
    "database_drop",
    "database_location",
    "database_properties",
    "date_2",
    "date_comparison",
    "decimal_1",
    "decimal_4",
    "decimal_join",
    "default_partition_name",
    "delimiter",
    "desc_non_existent_tbl",
    "disable_file_format_check",
    "distinct_stats",
    "drop_function",
    "drop_multi_partitions",
    "drop_table",
    "drop_table2",
    "drop_view",
    "dynamic_partition_skip_default",
    "escape_clusterby1",
    "escape_distributeby1",
    "escape_orderby1",
    "escape_sortby1",
    "fileformat_sequencefile",
    "fileformat_text",
    "filter_join_breaktask",
    "filter_join_breaktask2",
    "groupby1",
    "groupby11",
    "groupby12",
    "groupby1_limit",
    "groupby_grouping_sets2",
    "groupby_grouping_sets3",
    "groupby_grouping_sets4",
    "groupby_grouping_sets5",
    "groupby1_map",
    "groupby1_map_nomap",
    "groupby1_map_skew",
    "groupby1_noskew",
    "groupby2",
    "groupby2_limit",
    "groupby2_map",
    "groupby2_map_skew",
    "groupby2_noskew",
    "groupby4",
    "groupby4_map",
    "groupby4_map_skew",
    "groupby4_noskew",
    "groupby5",
    "groupby5_map",
    "groupby5_map_skew",
    "groupby5_noskew",
    "groupby6",
    "groupby6_map",
    "groupby6_map_skew",
    "groupby6_noskew",
    "groupby7",
    "groupby7_map",
    "groupby7_map_multi_single_reducer",
    "groupby7_map_skew",
    "groupby7_noskew",
    "groupby7_noskew_multi_single_reducer",
    "groupby8",
    "groupby8_map",
    "groupby8_map_skew",
    "groupby8_noskew",
    "groupby9",
    "groupby_distinct_samekey",
    "groupby_map_ppr",
    "groupby_multi_insert_common_distinct",
    "groupby_multi_single_reducer2",
    "groupby_multi_single_reducer3",
    "groupby_multi_insert_common_distinct",
    "groupby_neg_float",
    "groupby_ppd",
    "groupby_ppr",
    "groupby_sort_6",
    "having",
    "implicit_cast1",
    "index_serde",
    "infer_bucket_sort_dyn_part",
    "innerjoin",
    "input",
    "input0",
    "input1",
    "input10",
    "input11",
    "input11_limit",
    "input12",
    "input12_hadoop20",
    "input14",
    "input15",
    "input19",
    "input1_limit",
    "input2",
    "input21",
    "input22",
    "input23",
    "input24",
    "input25",
    "input26",
    "input28",
    "input2_limit",
    "input40",
    "input41",
    "input49",
    "input4_cb_delim",
    "input6",
    "input7",
    "input8",
    "input9",
    "input_limit",
    "input_part0",
    "input_part1",
    "input_part2",
    "input_part3",
    "input_part4",
    "input_part5",
    "input_part6",
    "input_part7",
    "input_part8",
    "input_part9",
    "input_testsequencefile",
    "inputddl1",
    "inputddl2",
    "inputddl3",
    "inputddl8",
    "insert1",
    "insert1_overwrite_partitions",
    "insert2_overwrite_partitions",
    "join1",
    "join10",
    "join11",
    "join12",
    "join13",
    "join14",
    "join14_hadoop20",
    "join15",
    "join16",
    "join17",
    "join18",
    "join19",
    "join2",
    "join20",
    "join21",
    "join22",
    "join23",
    "join24",
    "join25",
    "join26",
    "join27",
    "join28",
    "join29",
    "join3",
    "join30",
    "join31",
    "join32",
    "join32_lessSize",
    "join33",
    "join34",
    "join35",
    "join36",
    "join37",
    "join38",
    "join39",
    "join4",
    "join40",
    "join41",
    "join5",
    "join6",
    "join7",
    "join8",
    "join9",
    "join_1to1",
    "join_array",
    "join_casesensitive",
    "join_empty",
    "join_hive_626",
    "join_map_ppr",
    "join_rc",
    "join_reorder2",
    "join_reorder3",
    "join_reorder4",
    "join_star",
    "lateral_view",
    "lateral_view_noalias",
    "lateral_view_ppd",
    "leftsemijoin_mr",
    "limit_pushdown_negative",
    "lineage1",
    "literal_ints",
    "literal_string",
    "load_dyn_part1",
    "load_dyn_part10",
    "load_dyn_part11",
    "load_dyn_part12",
    "load_dyn_part13",
    "load_dyn_part14",
    "load_dyn_part14_win",
    "load_dyn_part3",
    "load_dyn_part4",
    "load_dyn_part5",
    "load_dyn_part6",
    "load_dyn_part7",
    "load_dyn_part8",
    "load_dyn_part9",
    "load_file_with_space_in_the_name",
    "loadpart1",
    "louter_join_ppr",
    "mapjoin_distinct",
    "mapjoin_filter_on_outerjoin",
    "mapjoin_mapjoin",
    "mapjoin_subquery",
    "mapjoin_test_outer",
    "mapreduce1",
    "mapreduce2",
    "mapreduce3",
    "mapreduce4",
    "mapreduce5",
    "mapreduce6",
    "mapreduce7",
    "mapreduce8",
    "merge1",
    "merge2",
    "merge4",
    "mergejoins",
    "multiMapJoin2",
    "multi_insert_gby",
    "multi_insert_gby3",
    "multi_insert_lateral_view",
    "multi_join_union",
    "multigroupby_singlemr",
    "noalias_subq1",
    "nonblock_op_deduplicate",
    "notable_alias1",
    "notable_alias2",
    "nullformatCTAS",
    "nullgroup",
    "nullgroup2",
    "nullgroup3",
    "nullgroup4",
    "nullgroup4_multi_distinct",
    "nullgroup5",
    "nullinput",
    "nullinput2",
    "nullscript",
    "optional_outer",
    "order",
    "order2",
    "outer_join_ppr",
    "parallel",
    "parenthesis_star_by",
    "part_inherit_tbl_props",
    "part_inherit_tbl_props_with_star",
    "partcols1",
    "partition_serde_format",
    "partition_type_check",
    "partition_wise_fileformat9",
    "ppd1",
    "ppd2",
    "ppd_clusterby",
    "ppd_constant_expr",
    "ppd_constant_where",
    "ppd_gby",
    "ppd_gby2",
    "ppd_gby_join",
    "ppd_join",
    "ppd_join2",
    "ppd_join3",
    "ppd_join_filter",
    "ppd_outer_join1",
    "ppd_outer_join2",
    "ppd_outer_join3",
    "ppd_outer_join4",
    "ppd_outer_join5",
    "ppd_random",
    "ppd_udf_col",
    "ppd_union",
    "ppr_allchildsarenull",
    "ppr_pushdown",
    "ppr_pushdown2",
    "ppr_pushdown3",
    "progress_1",
    "query_with_semi",
    "quote1",
    "quote2",
    "rcfile_columnar",
    "rcfile_lazydecompress",
    "rcfile_null_value",
    "rcfile_toleratecorruptions",
    "rcfile_union",
    "reduce_deduplicate_exclude_gby",
    "reduce_deduplicate_exclude_join",
    "reduce_deduplicate_extended",
    "router_join_ppr",
    "select_as_omitted",
    "select_unquote_and",
    "select_unquote_not",
    "select_unquote_or",
    "semicolon",
    "semijoin",
    "serde_regex",
    "set_variable_sub",
    "show_columns",
    "show_describe_func_quotes",
    "show_functions",
    "show_partitions",
    "show_tblproperties",
    "sort",
    "stats_aggregator_error_1",
    "stats_publisher_error_1",
    "subq2",
    "subquery_exists",
    "subquery_exists_having",
    "subquery_nonexistent",
    "subquery_nonexistent_having",
    "subquery_in_having",
    "tablename_with_select",
    "timestamp_comparison",
    "timestamp_null",
    "transform_ppr1",
    "transform_ppr2",
    "type_cast_1",
    "type_widening",
    "udaf_collect_set",
    "udaf_histogram_numeric",
    "udf2",
    "udf5",
    "udf6",
    "udf8",
    "udf9",
    "udf_10_trims",
    "udf_E",
    "udf_PI",
    "udf_acos",
    "udf_add",
    // "udf_array",  -- done in array.sql
    // "udf_array_contains",  -- done in array.sql
    "udf_ascii",
    "udf_asin",
    "udf_atan",
    "udf_avg",
    "udf_bigint",
    "udf_bin",
    "udf_bitmap_and",
    "udf_bitmap_empty",
    "udf_bitmap_or",
    "udf_bitwise_and",
    "udf_bitwise_not",
    "udf_bitwise_or",
    "udf_bitwise_xor",
    "udf_boolean",
    "udf_case",
    "udf_ceil",
    "udf_ceiling",
    "udf_concat",
    "udf_concat_insert1",
    "udf_concat_insert2",
    "udf_concat_ws",
    "udf_conv",
    "udf_cos",
    "udf_count",
    "udf_date_add",
    "udf_date_sub",
    "udf_datediff",
    "udf_day",
    "udf_dayofmonth",
    "udf_degrees",
    "udf_div",
    "udf_double",
    "udf_elt",
    "udf_equal",
    "udf_exp",
    "udf_find_in_set",
    "udf_float",
    "udf_floor",
    "udf_from_unixtime",
    "udf_greaterthan",
    "udf_greaterthanorequal",
    "udf_hex",
    "udf_if",
    "udf_index",
    "udf_instr",
    "udf_int",
    "udf_isnotnull",
    "udf_isnull",
    "udf_lcase",
    "udf_length",
    "udf_lessthan",
    "udf_lessthanorequal",
    "udf_like",
    "udf_ln",
    "udf_locate",
    "udf_log",
    "udf_log10",
    "udf_log2",
    "udf_lower",
    "udf_lpad",
    "udf_ltrim",
    "udf_map",
    "udf_modulo",
    "udf_month",
    "udf_named_struct",
    "udf_negative",
    "udf_not",
    "udf_notequal",
    "udf_notop",
    "udf_nvl",
    "udf_or",
    "udf_parse_url",
    "udf_pmod",
    "udf_positive",
    "udf_pow",
    "udf_power",
    "udf_rand",
    "udf_regexp",
    "udf_regexp_extract",
    "udf_regexp_replace",
    "udf_repeat",
    "udf_rlike",
    "udf_rpad",
    "udf_rtrim",
    "udf_sign",
    "udf_sin",
    "udf_smallint",
    "udf_space",
    "udf_sqrt",
    "udf_std",
    "udf_stddev",
    "udf_stddev_pop",
    "udf_stddev_samp",
    "udf_string",
    "udf_struct",
    "udf_substring",
    "udf_subtract",
    "udf_sum",
    "udf_tan",
    "udf_tinyint",
    "udf_to_byte",
    "udf_to_date",
    "udf_to_double",
    "udf_to_float",
    "udf_to_long",
    "udf_to_short",
    "udf_translate",
    "udf_trim",
    "udf_ucase",
    "udf_unix_timestamp",
    "udf_unhex",
    "udf_upper",
    "udf_var_pop",
    "udf_var_samp",
    "udf_variance",
    "udf_weekofyear",
    "udf_when",
    "union10",
    "union11",
    "union13",
    "union14",
    "union15",
    "union16",
    "union17",
    "union18",
    "union19",
    "union2",
    "union20",
    "union22",
    "union23",
    "union24",
    "union25",
    "union26",
    "union27",
    "union28",
    "union29",
    "union3",
    "union30",
    "union33",
    "union34",
    "union4",
    "union5",
    "union6",
    "union7",
    "union8",
    "union9",
    "union_lateralview",
    "union_ppr",
    "union_remove_6",
    "union_script",
    "varchar_union1",
    "view",
    "view_cast",
    "view_inputs"
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not on includeList or
   * excludeList are implicitly marked as ignored.
   */
  override def includeList: Seq[String] =
    commonIncludeList ++ Seq(
      "decimal_1_1"
    )
}
