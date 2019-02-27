# Difference with upstream

* [SPARK-21195](https://issues.apache.org/jira/browse/SPARK-21195) - Automatically register new metrics from sources and wire default registry
* [SPARK-20952](https://issues.apache.org/jira/browse/SPARK-20952) - ParquetFileFormat should forward TaskContext to its forkjoinpool
* [SPARK-20001](https://issues.apache.org/jira/browse/SPARK-20001) ([SPARK-13587](https://issues.apache.org/jira/browse/SPARK-13587)) - Support PythonRunner executing inside a Conda env (and R)
* [SPARK-17059](https://issues.apache.org/jira/browse/SPARK-17059) - Allow FileFormat to specify partition pruning strategy via splits
* [SPARK-24345](https://issues.apache.org/jira/browse/SPARK-24345) - Improve ParseError stop location when offending symbol is a token
* [SPARK-23795](https://issues.apache.org/jira/browse/SPARK-23795) - Make AbstractLauncher#self() protected
* [SPARK-18079](https://issues.apache.org/jira/browse/SPARK-18079) - CollectLimitExec.executeToIterator should perform per-partition limits

* [SPARK-15777](https://issues.apache.org/jira/browse/SPARK-15777) (Partial fix) - Catalog federation
    * make ExternalCatalog configurable beyond in memory and hive
    * FileIndex for catalog tables is provided by external catalog instead of using default impl

* Better pushdown for IN expressions in parquet via UserDefinedPredicate ([SPARK-17091](https://issues.apache.org/jira/browse/SPARK-17091) for original issue)
* SafeLogging implemented for the following files:
    * core: Broadcast, CoarseGrainedExecutorBackend, CoarseGrainedSchedulerBackend, Executor, MemoryStore, SparkContext, TorrentBroadcast
    * kubernetes: ExecutorPodsAllocator, ExecutorPodsLifecycleManager, ExecutorPodsPollingSnapshotSource, ExecutorPodsSnapshot, ExecutorPodsWatchSnapshotSource, KubernetesClusterSchedulerBackend
    * yarn: YarnClusterSchedulerBackend, YarnSchedulerBackend

* [SPARK-26626](https://issues.apache.org/jira/browse/SPARK-26626) - Limited the maximum size of repeatedly substituted aliases

# Added

* Gradle plugin to easily create custom docker images for use with k8s
* Filter rLibDir by exists so that daemon.R references the correct file [460](https://github.com/palantir/spark/pull/460)

# Reverted
* [SPARK-25908](https://issues.apache.org/jira/browse/SPARK-25908) - Removal of `monotonicall_increasing_id`, `toDegree`, `toRadians`, `approxCountDistinct`, `unionAll`
* [SPARK-25862](https://issues.apache.org/jira/browse/SPARK-25862) - Removal of `unboundedPreceding`, `unboundedFollowing`, `currentRow`
* [SPARK-26127](https://issues.apache.org/jira/browse/SPARK-26127) - Removal of deprecated setters from tree regression and classification models
* [SPARK-25867](https://issues.apache.org/jira/browse/SPARK-25867) - Removal of KMeans computeCost

* e59507243d Robert Kruszewski 14 seconds ago  (HEAD -> rk/merge-again) Revert "[SPARK-26216][SQL] Do not use case class as public API (UserDefinedFunction)"
* 8735a08f1b Robert Kruszewski 68 seconds ago  Revert "[SPARK-26216][SQL][FOLLOWUP] use abstract class instead of trait for UserDefinedFunction"
* 1423024322 Robert Kruszewski 2 minutes ago  Revert "[SPARK-26323][SQL] Scala UDF should still check input types even if some inputs are of type Any"
* b0d256d21a Robert Kruszewski 2 minutes ago  Revert "[SPARK-26580][SQL] remove Scala 2.11 hack for Scala UDF"