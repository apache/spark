package catalyst

/**
 * A partial reimplementation of Shark, a Hive compatible SQL engine running on Spark, using Catalyst.
 *
 * This implementation uses the hive parser, metadata catalog and serdes, but performs all optimization and execution
 * using catalyst and spark.
 *
 * Currently functions that are not supported by this implementation are passed back to the original Shark
 * implementation for execution.
 */
package object shark2