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
    "ops_comparison"
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not in whiteList
   * blacklist are implicitly marked as ignored.
   */
  val whiteList = Seq(
    "add_part_exist",
     "avro_change_schema",
     "avro_schema_error_message",
     "avro_schema_literal",
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
     "input0",
     "input21",
     "join_view",
     "literal_double",
     "literal_ints",
     "literal_string",
     "nestedvirtual",
     "quote2",
     "rename_column",
     "select_as_omitted",
     "set_variable_sub",
     "show_describe_func_quotes",
     "show_functions",
     "tablename_with_select",
     "udf_add",
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
     "udf_index",
     "udf_int",
     "udf_isnotnull",
     "udf_isnull",
     "udf_lcase",
     "udf_ln",
     "udf_log",
     "udf_log10",
     "udf_log2",
     "udf_ltrim",
     "udf_modulo",
     "udf_month",
     "udf_not",
     "udf_or",
     "udf_positive",
     "udf_pow",
     "udf_power",
     "udf_rand",
     "udf_regexp_extract",
     "udf_regexp_replace",
     "udf_rlike",
     "udf_rtrim",
     "udf_smallint",
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
     "udf_upper",
     "udf_var_pop",
     "udf_var_samp",
     "udf_variance",
     "union16"
  )

  // TODO: bundle in jar files... get from classpath
  val hiveQueryDir = new File(testShark.hiveDevHome, "ql/src/test/queries/clientpositive")
  val testCases = hiveQueryDir.listFiles
  val runAll = !(System.getProperty("shark.hive.alltests") == null)

  // Go through all the test cases and add them to scala test.
  testCases.foreach { testCase =>
    val testCaseName = testCase.getName.stripSuffix(".q")
    if(blackList contains testCaseName) {
      // Do nothing
    } else if(whiteList.contains(testCaseName)  || runAll) {
      // Build a test case and submit it to scala test framework...
      val queriesString = fileToString(testCase)
      createQueryTest(testCaseName, queriesString)
    } else {
      ignore(testCaseName) {}
    }
  }
}
