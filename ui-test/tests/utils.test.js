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


import * as utils from '../../core/src/main/resources/org/apache/spark/ui/static/utils.js';

test('ConvertDurationString', function () {
  expect(utils.ConvertDurationString('')).toBe(NaN);
  expect(utils.ConvertDurationString("2 h")).toBe(7200000);
  expect(utils.ConvertDurationString("2 min")).toBe(1200000);
  expect(utils.ConvertDurationString("2 s")).toBe(2000);
  expect(utils.ConvertDurationString("2 ms")).toBe(2);
});

test('formatDuration', function () {
  expect(utils.formatDuration(null)).toBe('NaN ms');
  expect(utils.formatDuration(0)).toBe('0.0 ms');
  expect(utils.formatDuration(99)).toBe('99.0 ms');
  expect(utils.formatDuration(999)).toBe('1.0 s');
  expect(utils.formatDuration(999 * 1000)).toBe('17 min');
  expect(utils.formatDuration(999 * 1000 * 1000)).toBe('277.5 h');
  expect(utils.formatDuration(999 * 1000 * 1000 * 1000)).toBe('277500.0 h');
});

test('formatBytes', function () {
  expect(utils.formatBytes(null)).toBe(null);
  expect(utils.formatBytes(null, 'display')).toBe('0.0 B');
  expect(utils.formatBytes(999, 'display')).toBe('999 B');
  expect(utils.formatBytes(999 * 1024, 'display')).toBe('999 KiB');
  expect(utils.formatBytes(999 * 1024 * 1024, 'display')).toBe('999 MiB');
  expect(utils.formatBytes(999 * 1024 * 1024 * 1024, 'display')).toBe('999 GiB');
  expect(utils.formatBytes(999 * 1024 * 1024 * 1024, 'display')).toBe('999 GiB');
  expect(utils.formatBytes(999 * 1024 * 1024 * 1024 * 1024, 'display')).toBe('999 TiB');
  expect(utils.formatBytes(999 * 1024 * 1024 * 1024 * 1024 * 1024, 'display')).toBe('999 PiB');
  expect(utils.formatBytes(999 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024, 'display')).toBe('999 EiB');
});

test('formatTimeMillis', function () {
  const timezoneOffset = new Date().getTimezoneOffset();
  const timezoneOffsetMillis = timezoneOffset * 60 * 1000;
  const mm = 60 * 1000;
  const mh = 60 * mm;
  const md = 24 * mh;
  expect(utils.formatTimeMillis(null)).toBe('-');
  expect(utils.formatTimeMillis(0)).toBe('-');
  expect(utils.formatTimeMillis(5 * md + timezoneOffsetMillis)).toBe('1970-01-06 00:00:00');
  expect(utils.formatTimeMillis(17852 * md + 13 * mh + 33 * mm + 33 * 1000 + timezoneOffsetMillis)).toBe('2018-11-17 13:33:33');
});

test('errorSummary', function () {
  const e1 = "Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 1) (10.221.98.22 executor driver): org.apache.spark.SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero. Use `try_divide` to tolerate divisor being 0 and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error.\n== SQL (line 1, position 8) ==\nselect a/b from src\n       ^^^\n\n\tat org.apache.spark.sql.errors.QueryExecutionErrors$.divideByZeroError(QueryExecutionErrors.scala:226)\n\tat org.apache.spark.sql.errors.QueryExecutionErrors.divideByZeroError(QueryExecutionErrors.scala)\n\tat org.apache.spark.sql.catalyst.expressions.GeneratedClass$GeneratedIteratorForCodegenStage1.processNext(generated.java:54)\n\tat org.apache.spark.sql.execution.BufferedRowIterator.hasNext(BufferedRowIterator.java:43)\n\tat org.apache.spark.sql.execution.WholeStageCodegenEvaluatorFactory$WholeStageCodegenPartitionEvaluator$$anon$1.hasNext(WholeStageCodegenEvaluatorFactory.scala:43)\n\tat org.apache.spark.sql.execution.SparkPlan.$anonfun$getByteArrayRdd$1(SparkPlan.scala:388)\n\tat org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2(RDD.scala:890)\n\tat org.apache.spark.rdd.RDD.$anonfun$mapPartitionsInternal$2$adapted(RDD.scala:890)\n\tat org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)\n\tat org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:364)\n\tat org.apache.spark.rdd.RDD.iterator(RDD.scala:328)\n\tat org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:93)\n\tat org.apache.spark.TaskContext.runTaskWithListeners(TaskContext.scala:161)\n\tat org.apache.spark.scheduler.Task.run(Task.scala:141)\n\tat org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$4(Executor.scala:592)\n\tat org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1474)\n\tat org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:595)\n\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)\n\tat java.lang.Thread.run(Thread.java:750)\n\nDriver stacktrace:";
  expect(utils.errorSummary(e1).toString()).toBe('DIVIDE_BY_ZERO,true');
  const e2 = "java.lang.RuntimeException: random text";
  expect(utils.errorSummary(e2).toString()).toBe('java.lang.RuntimeException,true');
});

test('stringAbbreviate', function () {
  expect(utils.stringAbbreviate(null, 10)).toBe(null);
  expect(utils.stringAbbreviate('1234567890', 10)).toBe('1234567890');
  expect(utils.stringAbbreviate('12345678901', 10)).toContain('1234567890...');
  expect(utils.stringAbbreviate('12345678901', 10)).toContain('<pre>12345678901</pre>')
});
