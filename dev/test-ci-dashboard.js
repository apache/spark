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

'use strict';

const assert = require('node:assert/strict');
const {test} = require('node:test');
const {calendar, parseCron, runnerLoad} = require('../docs/js/ci-dashboard.js');
const workload = (cron, extra = {}) => ({file: 'build.yml', branch: 'master', name: 'Build',
  scheduler: 'build.yml', crons: [cron], ...extra});

test('monthly phases retain February and 31-day boundary behavior', () => {
  const w = workload('17 6 1-28/3 * *');
  assert.equal(calendar([w], '2027-02').total, 10);
  assert.equal(calendar([w], '2028-02').total, 10);
  const march = calendar([w], '2027-03');
  assert.equal(march.cells[0][6][0].minute, 17);
  assert.equal(march.cells[30][6].length, 0);
  const phase3 = workload('17 6 3-31/3 * *');
  assert.equal(calendar([phase3], '2028-02').total, 9);
  assert.equal(calendar([phase3], '2027-03').total, 10);
});

test('weekly schedules, names, leap days, and cron day-of-month/day-of-week semantics', () => {
  assert.equal(calendar([workload('17 3 * * 0')], '2026-11').total, 5);
  assert.equal(calendar([workload('17 3 * NOV SUN')], '2026-11').total, 5);
  assert.equal(calendar([workload('17 3 * * 7')], '2026-11').total, 5);
  assert.equal(calendar([workload('0 0 29 FEB *')], '2028-02').total, 1);
  assert.equal(calendar([workload('0 0 29 FEB *')], '2027-02').total, 0);
  assert.equal(calendar([workload('0 0 1 * MON')], '2026-11').total, 6);
});

test('polling steps, ranges, and duplicate schedules', () => {
  assert.equal(calendar([workload('*/10 * * * *')], '2026-11').total, 4320);
  assert.equal(calendar([workload('35 2-22/4 * * *')], '2026-11').total, 180);
  const w = workload('0 0 * * *', {crons: ['0 0 * * *', '0 0 1 * *']});
  assert.equal(calendar([w], '2026-11').total, 30);
});

test('invalid crons and months fail explicitly', () => {
  for (const cron of ['0 0 * *', '*/0 * * * *', '60 * * * *', '0 24 * * *',
    '0 0 1-2-3 * *', '0 0 * * MONKEY', '*/x * * * *']) {
    assert.throws(() => parseCron(cron));
  }
  assert.throws(() => calendar([], '2026-13'));
});

const profiled = (cron, intervals, sampleCount = 1, branch = 'master') =>
  workload(cron, {branch, history: {intervals, sampleCount}});
const close = (a, b) => assert.ok(Math.abs(a - b) < 1e-8, `${a} != ${b}`);

test('runner load sums parallel job durations rather than workflow elapsed time', () => {
  const w = profiled('0 1 1 * *', [[0, 3600], [0, 3600]]);
  const data = runnerLoad([w], '2026-11');
  close(data.total, 2);
  close(data.cells[0][1], 2);
  close(data.byBranch.get('master'), 2);
});

test('job execution spills into later hours and successful samples are averaged', () => {
  const w = profiled('30 1 1 * *', [[0, 3600], [0, 10800]], 2);
  const data = runnerLoad([w], '2026-11');
  close(data.total, 2);
  close(data.cells[0][1], 0.5);
  close(data.cells[0][2], 0.75);
  close(data.cells[0][3], 0.5);
  close(data.cells[0][4], 0.25);
});

test('load includes previous-month spillover and clips jobs at the month end', () => {
  const w = profiled('30 23 31 * *', [[0, 3600]]);
  close(runnerLoad([w], '2027-02').total, 0.5);
  close(runnerLoad([w], '2027-03').total, 0.5);
  close(runnerLoad([w], '2027-02').cells[0][0], 0.5);
});

test('coverage excludes missing profiles and branch totals equal total load', () => {
  const a = profiled('0 0 1 * *', [[0, 3600]]);
  const b = profiled('0 1 1 * *', [[0, 7200]], 1, 'branch-4.x');
  const data = runnerLoad([a, b, workload('0 0 1 * *')], '2026-11');
  assert.equal(data.measured, 2);
  close(data.total, 3);
  close([...data.byBranch.values()].reduce((a, b) => a + b, 0), data.total);
  assert.equal(runnerLoad([workload('0 0 1 * *')], '2026-11').measured, 0);
});
