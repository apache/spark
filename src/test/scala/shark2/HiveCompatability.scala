package catalyst
package shark2

import java.io._

import util._

/**
 * A framework for running the query tests that are included in hive distribution.
 */
class HiveCompatability extends HiveComaparisionTest {
  /** A list of tests deemed out of scope and thus completely disregarded */
  val blackList = Seq(
    "hook_order", // These tests use hooks that are not on the classpath and thus break all subsequent SQL execution.
    "hook_context",
    "mapjoin_hook",
    "multi_sahooks",
    "overridden_confs",
    "query_properties",
    "sample10",
    "updateAccessTime",

    // Hive seems to think 1.0 > NaN = true && 1.0 < NaN = false... which is wrong.
    // http://stackoverflow.com/a/1573715
    "ops_comparison",

    // The skewjoin test seems to never complete on hive...
    "skewjoin",

    // This test fails and and exits the JVM.
    "auto_join18_multi_distinct",

    // Uses a serde that isn't on the classpath... breaks other tests.
    "bucketizedhiveinputformat"
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not in whiteList
   * blacklist are implicitly marked as ignored.
   */
  val whiteList = Seq(
    "add_part_exist",
    "auto_join23",
    "auto_join26",
    "auto_join28",
    "avro_change_schema",
    "avro_schema_error_message",
    "avro_schema_literal",
    "bucketmapjoin6",
    "count",
    "ct_case_insensitive",
    "database_properties",
    "default_partition_name",
    "delimiter",
    "describe_database_json",
    "drop_function",
    "drop_index",
    "drop_partitions_filter",
    "drop_partitions_filter2",
    "drop_partitions_filter3",
    "drop_table",
    "drop_view",
    "index_auth",
    //"index_auto_mult_tables",
    //"index_auto_mult_tables_compact",
    "index_auto_self_join",
    "innerjoin",
    "input0",
    "input4_limit",
    "insert1",
    "join0",
    "join1",
    "join10",
    "join11",
    "join12",
    "join13",
    "join15",
    "join16",
    "join19",
    "join22",
    "join23",
    "join25",
    "join29",
    "join30",
    "join31",
    "join34",
    "join36",
    "join37",
    "join_casesensitive",
    "join_empty",
    "join_view",
    "literal_double",
    "literal_ints",
    "literal_string",
    "mergejoins",
    "nestedvirtual",
    "no_hooks",
    "noalias_subq1",
    "nullgroup",
    "nullgroup2",
    "nullgroup3",
    "nullinput",
    "ppd_gby_join",
    "ppd_outer_join5",
    "ppd_random",
    "ppd_repeated_alias",
    "ppd_udf_col",
    "progress_1",
    "quote2",
    "rename_column",
    "select_as_omitted",
    "set_variable_sub",
    "show_describe_func_quotes",
    "show_functions",
    "smb_mapjoin_10",
    "tablename_with_select",
    "udf9",
    "udf_add",
    "udf_ascii",
    "udf_avg",
    "udf_bigint",
    "udf_bitwise_and",
    "udf_bitwise_not",
    "udf_bitwise_or",
    "udf_bitwise_xor",
    "udf_boolean",
    "udf_ceil",
    "udf_ceiling",
    "udf_date_add",
    "udf_date_sub",
    "udf_datediff",
    "udf_day",
    "udf_dayofmonth",
    "udf_double",
    "udf_exp",
    "udf_float",
    "udf_floor",
    "udf_from_unixtime",
    "udf_hour",
    "udf_index",
    "udf_int",
    "udf_isnotnull",
    "udf_isnull",
    "udf_lcase",
    "udf_length",
    "udf_ln",
    "udf_log",
    "udf_log10",
    "udf_log2",
    "udf_lower",
    "udf_lpad",
    "udf_ltrim",
    "udf_minute",
    "udf_modulo",
    "udf_month",
    "udf_not",
    "udf_or",
    "udf_parse_url",
    "udf_pmod",
    "udf_positive",
    "udf_pow",
    "udf_power",
    "udf_rand",
    "udf_regexp_extract",
    "udf_regexp_replace",
    "udf_repeat",
    "udf_rlike",
    "udf_rpad",
    "udf_rtrim",
    "udf_second",
    "udf_smallint",
    "udf_space",
    "udf_sqrt",
    "udf_std",
    "udf_stddev",
    "udf_stddev_pop",
    "udf_stddev_samp",
    "udf_string",
    "udf_substring",
    "udf_subtract",
    "udf_sum",
    "udf_tinyint",
    "udf_to_date",
    "udf_trim",
    "udf_ucase",
    "udf_unhex",
    "udf_unix_timestamp",
    "udf_upper",
    "udf_var_pop",
    "udf_var_samp",
    "udf_variance",
    "udf_weekofyear",
    "udf_xpath_boolean",
    "udf_xpath_double",
    "udf_xpath_float",
    "udf_xpath_int",
    "udf_xpath_long",
    "udf_xpath_short",
    "union10",
    "union11",
    "union13",
    "union14",
    "union16",
    "union2",
    "union20",
    "union28",
    "union29",
    "union30",
    "union4",
    "union5",
    "union8",
    "union9"
  )

  // TODO: bundle in jar files... get from classpath
  val hiveQueryDir = new File(testShark.hiveDevHome, "ql/src/test/queries/clientpositive")
  val testCases = hiveQueryDir.listFiles
  val runAll = !(System.getProperty("shark.hive.alltests") == null)

  // Allow the whitelist to be overriden by a system property
  val realWhiteList = Option(System.getProperty("shark.hive.whitelist")).map(_.split(",").toSeq).getOrElse(whiteList)

  // Go through all the test cases and add them to scala test.
  testCases.foreach { testCase =>
    val testCaseName = testCase.getName.stripSuffix(".q")
    if(blackList contains testCaseName) {
      // Do nothing
    } else if(realWhiteList.map(_.r.pattern.matcher(testCaseName).matches()).reduceLeft(_||_) || runAll) {
      // Build a test case and submit it to scala test framework...
      val queriesString = fileToString(testCase)
      createQueryTest(testCaseName, queriesString)
    } else {
      ignore(testCaseName) {}
    }
  }
}
