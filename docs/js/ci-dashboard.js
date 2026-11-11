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

  return {key, calendar, parseCron};
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
  const {calendar} = CIDashboard;
  let snapshot;
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
    node.append(element('strong', value.toLocaleString()), element('span', label));
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
  }
  function renderCalendar(cells, total) {
    const table = $('calendar');
    table.replaceChildren();
    const caption = element('caption', `${$('month').value} / UTC / configured target starts`);
    const head = element('thead'), headings = element('tr');
    headings.append(element('th', 'Date / UTC'));
    for (let hour = 0; hour < 24; hour++) {
      headings.append(element('th', String(hour).padStart(2, '0')));
    }
    headings.append(element('th', 'Total'));
    for (const th of headings.children) th.scope = 'col';
    head.append(headings);
    const body = element('tbody');
    const hourly = Array(24).fill(0);
    const peak = Math.max(1, ...cells.flat().map(events => events.length));
    cells.forEach((hours, day) => {
      const row = element('tr');
      const date = new Date(`${$('month').value}-${String(day + 1).padStart(2, '0')}T00:00:00Z`);
      const label = `${String(day + 1).padStart(2, '0')} ` +
        date.toLocaleDateString('en', {weekday: 'short', timeZone: 'UTC'});
      const heading = element('th', label);
      heading.scope = 'row';
      row.append(heading);
      hours.forEach((events, hour) => {
        const count = events.length;
        hourly[hour] += count;
        const css = count === 0 ? '' : count / peak > 0.66 ? 'high'
          : count / peak > 0.33 ? 'medium' : 'low';
        const cell = element('td', count || '', css);
        cell.title = `${label} ${hour}:00 UTC: ${count} target starts`;
        row.append(cell);
      });
      const daily = hours.reduce((sum, events) => sum + events.length, 0);
      row.append(element('td', daily, 'total'));
      body.append(row);
    });
    const foot = element('tfoot'), totals = element('tr');
    const label = element('th', 'Total');
    label.scope = 'row';
    totals.append(label);
    for (const value of hourly) totals.append(element('td', value, 'total'));
    totals.append(element('td', total, 'total'));
    foot.append(totals);
    table.append(caption, head, body, foot);
  }
  function render() {
    if (!snapshot || !$('month').value) return;
    error('');
    const workloads = snapshot.workloads.filter(w => !polling(w));
    const month = $('month').value;
    const data = calendar(workloads, month);
    const branches = [...new Set(workloads.map(w => w.branch))].sort();
    $('metrics').replaceChildren(
      metric(snapshot.workflows.length, 'Workflow files'),
      metric(branches.length, 'Scheduled branches'),
      metric(workloads.length, 'Scheduled targets'),
      metric(data.total, 'Target starts this month')
    );
    const daily = data.cells.map(hours =>
      hours.reduce((sum, events) => sum + events.length, 0));
    const peak = Math.max(0, ...data.cells.flat().map(events => events.length));
    const dailyRange = `${Math.min(...daily)}-${Math.max(...daily)} starts per day`;
    $('schedule-summary').textContent = dailyRange +
      ` / Peak: ${peak} starts in one UTC hour`;
    renderCalendar(data.cells, data.total);
    $('branches').replaceChildren();
    for (const branch of branches) {
      const targets = workloads.filter(w => w.branch === branch);
      const count = calendar(targets, month).total;
      const percent = data.total ? count / data.total * 100 : 0;
      const row = element('tr');
      const share = element('td'), content = element('div', undefined, 'share');
      const bar = element('span', undefined, 'bar');
      bar.style.width = `${percent}%`;
      content.append(element('span', `${percent.toFixed(1)}%`), bar);
      share.append(content);
      row.append(element('td', branch), element('td', targets.length), element('td', count), share);
      $('branches').append(row);
    }
    const polls = snapshot.workloads.filter(polling);
    $('polling').textContent = polls.length ? polls.map(w => w.crons.join('; ')).join('; ') +
      ` (UTC) / ${calendar(polls, month).total.toLocaleString()} configured starts this month.` :
      'No scheduled build-status polling in this revision.';
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
