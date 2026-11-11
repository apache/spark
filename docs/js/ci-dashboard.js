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

const CIDashboard = (() => {
  const key = w => `${w.file}@${w.branch}`;
  const months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
    'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
  const weekdays = ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'];

  function field(source, min, max, names = []) {
    const values = new Set();
    const number = token => {
      const index = names.indexOf(token.toUpperCase());
      if (index >= 0) return index + min;
      if (!/^\d+$/.test(token)) throw new Error(`Unsupported cron field: ${source}`);
      return Number(token);
    };
    for (const part of source.split(',')) {
      const pieces = part.split('/');
      if (pieces.length > 2 || (pieces.length === 2 && !/^\d+$/.test(pieces[1]))) {
        throw new Error(`Invalid cron step: ${source}`);
      }
      const step = pieces.length === 2 ? Number(pieces[1]) : 1;
      const range = pieces[0].split('-');
      if (range.length > 2) throw new Error(`Invalid cron range: ${source}`);
      const start = pieces[0] === '*' ? min : number(range[0]);
      const end = pieces[0] === '*' ? max : range.length === 2
        ? number(range[1]) : pieces.length === 2 ? max : start;
      if (step < 1 || start < min || end > max || start > end) {
        throw new Error(`Invalid cron range: ${source}`);
      }
      for (let value = start; value <= end; value += step) values.add(value);
    }
    return values;
  }

  function parseCron(expression) {
    const fields = expression.trim().split(/\s+/);
    if (fields.length !== 5) throw new Error(`Expected a five-field cron: ${expression}`);
    const [minute, hour, day, month, weekday] = fields;
    return {
      minutes: field(minute, 0, 59),
      hours: field(hour, 0, 23),
      days: field(day, 1, 31),
      months: field(month, 1, 12, months),
      weekdays: new Set([...field(weekday, 0, 7, weekdays)].map(d => d % 7)),
      dayWildcard: day.startsWith('*'),
      weekdayWildcard: weekday.startsWith('*')
    };
  }

  function calendar(workloads, month) {
    if (!/^\d{4}-\d{2}$/.test(month)) throw new Error('Choose a valid month.');
    const [year, monthNumber] = month.split('-').map(Number);
    if (year < 1000 || monthNumber < 1 || monthNumber > 12) {
      throw new Error('Choose a valid month.');
    }
    const days = new Date(Date.UTC(year, monthNumber, 0)).getUTCDate();
    const cells = Array.from({length: days}, () => Array.from({length: 24}, () => []));
    const counts = new Map();
    for (const workload of workloads) {
      const seen = new Set();
      for (const expression of workload.crons) {
        const cron = parseCron(expression);
        if (!cron.months.has(monthNumber)) continue;
        for (let day = 1; day <= days; day++) {
          const weekday = new Date(Date.UTC(year, monthNumber - 1, day)).getUTCDay();
          const dm = cron.days.has(day), dw = cron.weekdays.has(weekday);
          const matches = cron.dayWildcard || cron.weekdayWildcard ? dm && dw : dm || dw;
          if (!matches) continue;
          for (const hour of cron.hours) {
            for (const minute of cron.minutes) {
              const instant = `${day}:${hour}:${minute}`;
              if (seen.has(instant)) continue;
              seen.add(instant);
              cells[day - 1][hour].push({workload, minute});
            }
          }
        }
      }
      counts.set(key(workload), seen.size);
    }
    return {cells, counts, total: [...counts.values()].reduce((a, b) => a + b, 0)};
  }

  function runnerLoad(workloads, month) {
    const days = calendar([], month).cells.length;
    const first = Date.parse(`${month}-01T00:00:00Z`);
    const end = first + days * 86400000;
    const hourMs = 3600000;
    const measured = workloads.filter(w => w.history?.sampleCount > 0);
    const cells = Array.from({length: days}, () => Array(24).fill(0));
    const byBranch = new Map();
    const byBranchCells = new Map();
    let lookback = 0;
    for (const w of measured) {
      for (const [offset, duration] of w.history.intervals) {
        lookback = Math.max(lookback, (offset + duration) * 1000);
      }
    }
    const cursor = new Date(first - lookback);
    cursor.setUTCDate(1);
    cursor.setUTCHours(0, 0, 0, 0);
    while (cursor.getTime() < end) {
      const start = cursor.getTime();
      const schedule = calendar(measured, cursor.toISOString().slice(0, 7));
      schedule.cells.forEach((hours, day) => hours.forEach((events, hour) => {
        for (const {workload, minute} of events) {
          const origin = start + ((day * 24 + hour) * 60 + minute) * 60000;
          for (const [offset, duration] of workload.history.intervals) {
            const from = Math.max(first, origin + offset * 1000);
            const to = Math.min(end, origin + (offset + duration) * 1000);
            if (to <= from) continue;
            const begin = Math.floor((from - first) / hourMs);
            const finish = Math.ceil((to - first) / hourMs);
            for (let slot = begin; slot < finish; slot++) {
              const overlap = Math.min(to, first + (slot + 1) * hourMs) -
                Math.max(from, first + slot * hourMs);
              const value = overlap / hourMs / workload.history.sampleCount;
              cells[Math.floor(slot / 24)][slot % 24] += value;
              if (!byBranchCells.has(workload.branch)) {
                byBranchCells.set(workload.branch,
                  Array.from({length: days}, () => Array(24).fill(0)));
              }
              byBranchCells.get(workload.branch)[Math.floor(slot / 24)][slot % 24] += value;
              byBranch.set(workload.branch, (byBranch.get(workload.branch) || 0) + value);
            }
          }
        }
      }));
      cursor.setUTCMonth(cursor.getUTCMonth() + 1);
    }
    return {cells, byBranch, byBranchCells, measured: measured.length,
      total: cells.flat().reduce((sum, value) => sum + value, 0)};
  }

  function actualForecast(snapshot, month) {
    const workloads = snapshot.workloads.filter(w => w.file !== 'update_build_status.yml');
    const estimate = runnerLoad(workloads, month);
    const first = Date.parse(`${month}-01T00:00:00Z`);
    const until = snapshot.history?.until || snapshot.generatedAt;
    const today = until.slice(0, 10);
    const records = snapshot.history?.observed?.days || {};
    const complete = Object.entries(records).filter(([day, value]) =>
      day < today && Date.parse(value.through) >= Date.parse(day) + 86400000);
    const observedEnd = Math.max(0, ...Object.values(records).map(v => Date.parse(v.through)));
    const byBranch = new Map();
    const branch = name => {
      if (!byBranch.has(name)) byBranch.set(name, {actual: 0, forecast: 0});
      return byBranch.get(name);
    };
    let actualTotal = 0, forecastTotal = 0, actualDays = 0, futureDays = 0, unknownDays = 0;
    const types = [];
    const cells = estimate.cells.map((hours, index) => {
      const date = new Date(first + index * 86400000);
      const day = date.toISOString().slice(0, 10);
      const observed = records[day];
      const values = Array(24).fill(null);
      const kinds = Array(24).fill('unknown');
      if (day <= today && observed) {
        actualDays++;
        for (let hour = 0; hour < 24; hour++) {
          if (date.getTime() + hour * 3600000 >= Date.parse(observed.through)) continue;
          values[hour] = observed.hours[hour];
          kinds[hour] = 'actual';
          actualTotal += values[hour];
        }
        for (const [name, values] of Object.entries(observed.byBranch)) {
          branch(name).actual += values.reduce((a, b) => a + b, 0);
        }
      } else if (day > today) {
        futureDays++;
        const weekday = date.getUTCDay();
        const sameWeekday = complete.filter(([day]) => new Date(day).getUTCDay() === weekday);
        const baseline = sameWeekday.length ? sameWeekday : complete;
        if (estimate.measured || baseline.length) {
          for (let hour = 0; hour < 24; hour++) {
            const push = baseline.reduce((sum, [, v]) => sum + v.pushHours[hour], 0) /
              (baseline.length || 1);
            values[hour] = hours[hour] + push;
            kinds[hour] = 'forecast';
            forecastTotal += values[hour];
          }
          for (const [name, days] of estimate.byBranchCells) {
            branch(name).forecast += days[index].reduce((a, b) => a + b, 0);
          }
          for (const [, record] of baseline) {
            for (const [name, values] of Object.entries(record.pushByBranch)) {
              branch(name).forecast += values.reduce((a, b) => a + b, 0) / baseline.length;
            }
          }
        }
      } else {
        unknownDays++;
      }
      types.push(kinds);
      return values;
    });
    return {cells, types, byBranch, actualTotal, forecastTotal, actualDays, futureDays,
      unknownDays, baselineDays: complete.length, profiled: estimate.measured, today,
      observedUntil: observedEnd ? new Date(observedEnd).toISOString() : null};
  }

  return {key, calendar, parseCron, runnerLoad, actualForecast};
})();

if (typeof module !== 'undefined') module.exports = CIDashboard;

if (typeof document !== 'undefined') {
  const $ = id => document.getElementById(id);
  const element = (tag, text, className) => {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = text;
    if (className) node.className = className;
    return node;
  };
  const {calendar, actualForecast} = CIDashboard;
  let snapshot;
  const format = value => value.toLocaleString('en', {maximumFractionDigits: 1});
  const error = message => {
    $('error').textContent = message;
    $('error').hidden = !message;
  };
  const source = file => {
    const link = element('a', file);
    link.href = `https://github.com/apache/spark/blob/${encodeURIComponent(snapshot.revision)}` +
      `/.github/workflows/${encodeURIComponent(file)}`;
    return link;
  };
  const polling = w => w.file === 'update_build_status.yml';
  function metric(value, label) {
    const node = element('div', undefined, 'metric');
    node.append(element('strong', typeof value === 'number' ? format(value) : value),
      element('span', label));
    return node;
  }
  function overview() {
    const labels = {push: 'Push', schedule: 'Scheduled', workflow_dispatch: 'Manual',
      workflow_call: 'Reusable', workflow_run: 'On workflow completion'};
    const events = new Map();
    for (const w of snapshot.workflows) {
      for (const event of w.events) events.set(event, (events.get(event) || 0) + 1);
    }
    $('triggers').replaceChildren();
    for (const [name, count] of [...events].sort()) {
      $('triggers').append(element('span', `${labels[name] || name}: ${count} workflows`));
    }
    $('engines').replaceChildren();
    for (const w of snapshot.workflows.filter(w => w.events.includes('workflow_call'))) {
      const card = element('div', undefined, 'flow');
      const callers = snapshot.workflows
        .filter(candidate => candidate.calls.includes(w.file)).length;
      card.append(element('h3', w.name), source(w.file),
        element('p', `${callers} callers / ${w.jobCount} job templates`));
      $('engines').append(card);
    }
    const push = snapshot.history?.push;
    if (push) {
      $('push-metrics').replaceChildren(
        metric(push.runnerHours, 'Measured push runner-hours'),
        metric(push.runCount, 'Upstream push runs'),
        metric(push.jobCount, 'Job executions, including reruns'),
        metric(push.runCount ? push.runnerHours / push.runCount : 0, 'Runner-hours per run')
      );
      $('push-status').textContent = `${push.since.slice(0, 10)} to ${push.until} / ` +
        `Includes ${push.failedRuns} failed runs and active-job execution so far. ` +
        'Usage is assigned to execution hours, including jobs crossing the window boundary.';
    } else {
      $('push-status').textContent = 'Upstream push measurements are unavailable. ' +
        (snapshot.history?.pushError || 'The Pages deployment collects recent Actions timings.');
    }
  }
  function renderCalendar(values, types, countMode) {
    const table = $('calendar');
    table.replaceChildren();
    const unit = countMode ? 'configured starts' : 'runner-hours';
    const caption = element('caption', `${$('month').value} / UTC / ${unit}`);
    const head = element('thead'), headings = element('tr');
    headings.append(element('th', 'Date / UTC'));
    for (let hour = 0; hour < 24; hour++) {
      headings.append(element('th', String(hour).padStart(2, '0')));
    }
    headings.append(element('th', 'Total'));
    for (const th of headings.children) th.scope = 'col';
    head.append(headings);
    const body = element('tbody');
    const hourly = Array(24).fill(0), known = Array(24).fill(false);
    const peak = Math.max(Number.EPSILON, ...values.flat().filter(v => v !== null));
    values.forEach((hours, day) => {
      const row = element('tr');
      const date = new Date(`${$('month').value}-${String(day + 1).padStart(2, '0')}T00:00:00Z`);
      const label = `${String(day + 1).padStart(2, '0')} ` +
        date.toLocaleDateString('en', {weekday: 'short', timeZone: 'UTC'});
      const heading = element('th', label);
      heading.scope = 'row';
      const kind = types[day].find(type => type !== 'unknown') || 'unknown';
      if (!countMode) heading.append(element('small', kind === 'actual' ? 'Actual' :
        kind === 'forecast' ? 'Forecast' : 'Unavailable'));
      row.append(heading);
      hours.forEach((value, hour) => {
        const type = types[day][hour];
        const shade = !value ? '' : value / peak > 0.66 ? 'high'
          : value / peak > 0.33 ? 'medium' : 'low';
        const text = value === null ? '' : value > 0 && value < 0.1 ? '<0.1' : format(value);
        const cell = element('td', text, `${type} ${shade}`);
        if (value !== null) {
          hourly[hour] += value;
          known[hour] = true;
        }
        cell.title = value === null ? `${label} ${hour}:00 UTC: unavailable` :
          `${label} ${hour}:00 UTC: ${type} ${value.toFixed(2)} ${unit}`;
        row.append(cell);
      });
      const available = hours.filter(v => v !== null);
      const daily = available.reduce((sum, count) => sum + count, 0);
      row.append(element('td', available.length ? format(daily) : 'n/a', 'total'));
      body.append(row);
    });
    const foot = element('tfoot'), totals = element('tr');
    const label = element('th', 'Shown total');
    label.scope = 'row';
    totals.append(label);
    hourly.forEach((value, index) => totals.append(element('td',
      known[index] ? format(value) : 'n/a', 'total')));
    totals.append(element('td', known.some(Boolean) ? format(hourly.reduce((a, b) => a + b)) :
      'n/a', 'total'));
    foot.append(totals);
    table.append(caption, head, body, foot);
  }
  function render() {
    if (!snapshot || !$('month').value) return;
    error('');
    const workloads = snapshot.workloads.filter(w => !polling(w));
    const month = $('month').value;
    const data = actualForecast(snapshot, month);
    const countMode = !snapshot.history;
    const configured = calendar(workloads, month);
    const values = countMode ? configured.cells.map(hours => hours.map(c => c.length)) : data.cells;
    const types = countMode ? values.map(hours => hours.map(() => 'configured')) : data.types;
    const available = data.cells.flat().filter(v => v !== null);
    const hasForecast = data.types.flat().includes('forecast');
    $('metrics').replaceChildren(
      metric(data.actualDays ? data.actualTotal : 'Unavailable', 'Actual runner-hours shown'),
      metric(hasForecast ? data.forecastTotal : data.futureDays ? 'Unavailable' : 0,
        'Forecast runner-hours for future days'),
      metric(available.length ? Math.max(...available) : 'Unavailable',
        'Peak shown hourly average runners'),
      metric(`${data.profiled}/${workloads.length}`, 'Scheduled targets with duration profiles')
    );
    $('schedule-summary').textContent = countMode ? 'Timings unavailable: configured starts only.' :
      `${data.actualDays} observed days / ${data.futureDays} future days / ` +
      `${data.unknownDays} past days without measurements. Today is measured so far.`;
    $('load-status').textContent = countMode
      ? 'The Pages deployment collects actual timestamps. No actual burden is inferred from crons.'
      : `Actual measurements end at ${data.observedUntil || 'unavailable'}. ` +
        'Forecasts start after ' +
        `${data.today} UTC and combine scheduled job profiles with a push baseline from ` +
        `${data.baselineDays} complete observed days, averaged by weekday and UTC hour. ` +
        `${data.profiled}/${workloads.length} scheduled targets are profiled; ` +
        'unprofiled targets and future manual runs are unmodeled. ' +
        (snapshot.history.actualError
          ? 'Latest measurement refresh failed; retained data is shown.' : '');
    $('load-legend').hidden = countMode;
    renderCalendar(values, types, countMode);
    $('branches').replaceChildren();
    const branches = new Set([...workloads.map(w => w.branch), ...data.byBranch.keys()]);
    for (const name of [...branches].sort()) {
      const targets = workloads.filter(w => w.branch === name);
      const measured = targets.filter(w => w.history?.sampleCount > 0).length;
      const cost = data.byBranch.get(name) || {actual: 0, forecast: 0};
      const row = element('tr');
      row.append(element('td', name), element('td', `${measured}/${targets.length}`),
        element('td', data.actualDays ? format(cost.actual) : 'Unavailable'),
        element('td', hasForecast ? format(cost.forecast) +
          (measured < targets.length ? ' + unprofiled' : '') : 'Unavailable'));
      $('branches').append(row);
    }
    const polls = snapshot.workloads.filter(polling);
    $('polling').textContent = polls.length ? polls.map(w => w.crons.join('; ')).join('; ') +
      ` (UTC) / ${format(calendar(polls, month).total)} configured starts this month. ` +
      'Excluded from the build burden timeline.' : 'No scheduled polling in this revision.';
  }
  function safeRender() {
    try { render(); } catch (e) { error(e.message); }
  }
  $('month').value = new Date().toISOString().slice(0, 7);
  $('month').addEventListener('change', safeRender);
  fetch('ci-dashboard-data.json').then(response => {
    if (!response.ok) throw new Error(`Workflow metadata request failed: ${response.status}`);
    return response.json();
  }).then(data => {
    snapshot = data;
    $('snapshot').textContent = `Source: ${data.revision.slice(0, 12)}` +
      ` / Updated ${data.generatedAt}`;
    overview();
    $('dashboard').hidden = false;
    safeRender();
  }).catch(e => error(`${e.message} Generate metadata with dev/generate-ci-dashboard.py ` +
    'or build the documentation with Jekyll, then serve the page over HTTP.'));
}
