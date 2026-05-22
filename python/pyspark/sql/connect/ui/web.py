#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Flask app for the local Spark Connect client UI.

The page is a thin shell that polls a JSON endpoint (``/api/sql.json``) and re-renders
the table, plan DAG, and progress bar entirely client-side. We pick that approach over
``<meta http-equiv="refresh">`` so that ``<details>`` elements the user has expanded
stay expanded across refreshes (their open state is persisted in ``sessionStorage``).
"""

from __future__ import annotations

import dataclasses
import datetime as _dt
import os
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from flask import Flask, jsonify, render_template_string

from pyspark.sql.connect.ui import list_sql_executions

if TYPE_CHECKING:
    from pyspark.sql.connect.session import SparkSession
    from pyspark.sql.connect.ui import _UIProgressHandler, SqlExecutionSummary


# Poll the API often enough that short queries don't slip between cycles. The Connect
# progress stream only emits events while a query is running, so the bar would be
# invisible for any query shorter than the poll interval if we matched the table's
# stated "refresh every N seconds" cadence.
_API_POLL_MS = 1000


_SHELL = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spark Connect UI</title>
  <style>
    body { font-family: -apple-system, system-ui, sans-serif; margin: 1.5em;
           color: #222; }
    h1 { margin-bottom: 0.25em; }
    .meta { color: #666; font-size: 0.9em; margin-bottom: 1em; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 0.45em 0.6em; border-bottom: 1px solid #eee;
             text-align: left; vertical-align: top; font-size: 0.9em; }
    th { background: #fafafa; font-weight: 600; }
    tr.detail-row td { background: #fbfbfb; border-bottom: 1px solid #eee; }
    .status-RUNNING   { color: #1565c0; font-weight: 600; }
    .status-COMPLETED { color: #2e7d32; }
    .status-FAILED    { color: #c62828; font-weight: 600; }
    .badge { display: inline-block; padding: 0.05em 0.45em; font-size: 0.8em;
             border-radius: 3px; background: #eef2ff; color: #3730a3; }
    .mono { font-family: SFMono-Regular, Menlo, Consolas, monospace; }
    summary { cursor: pointer; color: #1565c0; font-size: 0.85em;
              user-select: none; }
    summary::-webkit-details-marker { color: #888; }
    pre { margin: 0.3em 0 0 0; padding: 0.5em; background: #f4f4f4;
          border-radius: 3px; white-space: pre-wrap; word-break: break-word;
          font-size: 0.85em; }
    pre.error { background: #fdecea; color: #861818; }
    code { background: #f4f4f4; padding: 0.05em 0.25em; border-radius: 3px; }
    .muted { color: #888; }

    /* Inline progress bar (inside status cell) */
    .prog-wrap { margin-top: 0.3em; }
    .prog-bar {
      position: relative; height: 10px; background: #e0e7ff;
      border-radius: 3px; overflow: hidden; width: 120px;
    }
    .prog-fill {
      height: 100%; width: 0%; background: #4f46e5;
      transition: width 250ms ease-out;
    }
    .prog-fill.indeterminate {
      width: 35% !important; animation: indet 1.4s ease-in-out infinite;
    }
    @keyframes indet {
      0%   { transform: translateX(-100%); }
      100% { transform: translateX(310%); }
    }
    .prog-label { font-size: 0.78em; color: #4b5563; margin-top: 0.15em; }
    .prog-stages { font-size: 0.75em; color: #9ca3af; margin-top: 0.1em; }

    /* Execution info (metrics DAG + flows) */
    .ei-wrap { padding: 0.4em 0 0.6em 0; }
    .ei-section { margin-bottom: 0.7em; }
    .ei-section-title { font-size: 0.8em; font-weight: 600; color: #374151;
                        text-transform: uppercase; letter-spacing: 0.04em;
                        margin-bottom: 0.3em; }
    svg.mdag .mbox       { fill: #fff; stroke: #6366f1; stroke-width: 1.2; }
    svg.mdag .mbox-root  { stroke: #4f46e5; stroke-width: 1.8; }
    svg.mdag .mname      { font: 600 11px -apple-system, system-ui, sans-serif; fill: #1f2937; }
    svg.mdag .mval       { font: 10px SFMono-Regular, Menlo, Consolas, monospace; fill: #4b5563; }
    svg.mdag .mlabel     { font: 10px SFMono-Regular, Menlo, Consolas, monospace; fill: #9ca3af; }
    svg.mdag .medge      { stroke: #94a3b8; stroke-width: 1.2; fill: none; }
    svg.mdag .marrow     { fill: #94a3b8; }
    .flows-table { font-size: 0.82em; border-collapse: collapse; margin-top: 0.3em; }
    .flows-table th { background: #f3f4f6; padding: 0.25em 0.6em; font-weight: 600;
                      border: 1px solid #e5e7eb; color: #374151; }
    .flows-table td { padding: 0.2em 0.6em; border: 1px solid #e5e7eb; }
    .flows-table td.mono { font-family: SFMono-Regular, Menlo, Consolas, monospace; }
    .tag-badge { display: inline-block; padding: 0.05em 0.4em; margin: 0 0.2em 0.1em 0;
                 font-size: 0.78em; border-radius: 3px; background: #f0fdf4;
                 color: #166534; border: 1px solid #bbf7d0;
                 font-family: SFMono-Regular, Menlo, Consolas, monospace; }

    /* Plan DAG (classic-UI-flavored) */
    .dag-wrap { overflow-x: auto; padding: 4px 2px 8px 2px; }
    svg.dag { display: block; }
    svg.dag .nbox { fill: #fff; stroke: #6366f1; stroke-width: 1.4; }
    svg.dag .nbox-leaf { stroke: #0e7490; }
    svg.dag .nbox-binary { stroke: #b45309; }
    svg.dag .ntitle { font: 600 12px -apple-system, system-ui, sans-serif;
                      fill: #1f2937; }
    svg.dag .nsub   { font: 11px SFMono-Regular, Menlo, Consolas, monospace;
                      fill: #4b5563; }
    svg.dag .edge   { stroke: #94a3b8; stroke-width: 1.4; fill: none; }
    svg.dag .arrow  { fill: #94a3b8; }
    .dag-empty { color: #888; font-style: italic; }

    .empty { color: #888; padding: 1em 0; }
    .error-banner {
      background: #fdecea; color: #861818; padding: 0.6em 0.8em;
      border-radius: 3px; margin-bottom: 1em; display: none;
    }
  </style>
</head>
<body>
  <h1>Executions</h1>
  <div class="meta">
    Connected to <code>{{ remote }}</code>.
    User <span id="hdr-user" class="mono">-</span>
    &middot; Session <span id="hdr-session" class="mono">-</span>
    &middot; Last update <span id="fetched-at">-</span>
    &middot; Showing <span id="row-count">0</span> executions.
  </div>
  <div id="error" class="error-banner"></div>
  <div id="table-wrapper">
    <table>
      <thead>
        <tr>
          <th>ID</th>
          <th>Query ID</th>
          <th>User</th>
          <th>Tags</th>
          <th>Status</th>
          <th>Submitted</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody id="rows">
      </tbody>
    </table>
    <div id="empty" class="empty">No SQL executions yet.</div>
  </div>

  <script>
  /* eslint-env browser */
  (function () {
    var POLL_MS = {{ poll_ms }};

    function escapeHtml(s) {
      if (s === null || s === undefined) return "";
      return String(s)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    // ---- <details> open-state persistence (sessionStorage) ------------------
    // Each <details> gets a stable `data-detkey` so it survives re-render.
    document.body.addEventListener("toggle", function (ev) {
      var d = ev.target;
      if (!(d instanceof HTMLDetailsElement)) return;
      var k = d.dataset.detkey;
      if (!k) return;
      try { sessionStorage.setItem(k, d.open ? "1" : "0"); } catch (_) {}
    }, true);

    function applyOpenState(root) {
      var nodes = root.querySelectorAll("details[data-detkey]");
      for (var i = 0; i < nodes.length; i++) {
        var d = nodes[i];
        var saved;
        try { saved = sessionStorage.getItem(d.dataset.detkey); } catch (_) {}
        if (saved === "1") d.open = true;
        else if (saved === "0") d.open = false;
      }
    }

    // ---- Time/byte formatting ---------------------------------------------
    function pad(n) { return n < 10 ? "0" + n : "" + n; }
    function fmtTime(ms) {
      if (!ms) return "";
      var d = new Date(ms);
      return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate())
        + " " + pad(d.getHours()) + ":" + pad(d.getMinutes()) + ":" + pad(d.getSeconds());
    }
    function fmtDuration(startMs, endMs) {
      if (!startMs) return "";
      var end = endMs || Date.now();
      var s = Math.max(0, (end - startMs) / 1000);
      if (s < 60) return s.toFixed(1) + "s";
      var m = Math.floor(s / 60);
      return m + "m " + Math.floor(s - m * 60) + "s";
    }
    var SI = [
      { v: 1024 * 1024 * 1024 * 1024, s: "TiB" },
      { v: 1024 * 1024 * 1024, s: "GiB" },
      { v: 1024 * 1024, s: "MiB" },
      { v: 1024, s: "KiB" },
    ];
    function fmtBytes(n) {
      n = +n || 0;
      for (var i = 0; i < SI.length; i++) {
        if (n >= 2 * SI[i].v) return (n / SI[i].v).toFixed(1) + " " + SI[i].s;
      }
      return n + " B";
    }

    // ---- Proto-text tokenizer & parser ------------------------------------
    // The Connect server stores the call-site as proto-text-format of the
    // ExecutePlanRequest. We tokenize it once, then walk the `plan` subtree
    // to build a DAG.
    function tokenize(text) {
      var t = [];
      var i = 0, n = text.length;
      while (i < n) {
        var c = text.charAt(i);
        if (c === " " || c === "\\n" || c === "\\t" || c === "\\r") { i++; continue; }
        if (c === "{" || c === "}" || c === ":") {
          t.push({ k: c, v: c }); i++; continue;
        }
        if (c === '"') {
          var s = ""; var j = i + 1;
          while (j < n && text.charAt(j) !== '"') {
            if (text.charAt(j) === "\\\\" && j + 1 < n) { s += text.charAt(j + 1); j += 2; }
            else { s += text.charAt(j); j++; }
          }
          t.push({ k: "str", v: s });
          i = j + 1; continue;
        }
        if (c === "." && text.substr(i, 3) === "...") {
          t.push({ k: "trunc", v: "..." }); i += 3; continue;
        }
        if (/[A-Za-z_]/.test(c)) {
          var p = i;
          while (i < n && /[A-Za-z0-9_]/.test(text.charAt(i))) i++;
          t.push({ k: "id", v: text.substring(p, i) });
          continue;
        }
        if (/[-0-9]/.test(c)) {
          var pp = i;
          while (i < n && /[-0-9eE.+]/.test(text.charAt(i))) i++;
          t.push({ k: "num", v: text.substring(pp, i) });
          continue;
        }
        i++;
      }
      return t;
    }

    function parseFields(toks, i) {
      var out = [];
      while (i < toks.length) {
        var t = toks[i];
        if (t.k === "}") return { fields: out, next: i + 1 };
        if (t.k === "trunc") { out.push({ kind: "trunc" }); i++; continue; }
        if (t.k !== "id") { i++; continue; }
        var key = t.v;
        var next = toks[i + 1];
        if (!next) break;
        if (next.k === ":") {
          var v = toks[i + 2];
          if (!v) break;
          out.push({ kind: "scalar", key: key, vtype: v.k, value: v.v });
          i += 3;
        } else if (next.k === "{") {
          var inner = parseFields(toks, i + 2);
          out.push({ kind: "block", key: key, children: inner.fields });
          i = inner.next;
        } else {
          i++;
        }
      }
      return { fields: out, next: i };
    }

    function parseProto(text) {
      try { return parseFields(tokenize(text || ""), 0).fields; }
      catch (_) { return []; }
    }

    // ---- DAG extraction from parsed proto ---------------------------------
    // Returns the `plan.{root|command}` block, or null.
    function findPlanBlock(fields) {
      for (var i = 0; i < fields.length; i++) {
        var f = fields[i];
        if (f.kind === "block" && f.key === "plan") {
          for (var j = 0; j < f.children.length; j++) {
            var c = f.children[j];
            if (c.kind === "block" && (c.key === "root" || c.key === "command")) return c;
          }
        }
      }
      return null;
    }

    // Field names on Relation / Command messages that point at child relations.
    // Walked from sql/connect/common/.../{relations,commands,pipelines}.proto so
    // every variant's sub-plans show up in the DAG.
    var CHILD_FIELDS = {
      // Most Relation variants
      "input":               "input",
      // Binary relations (Join family, SetOperation)
      "left":                "left",
      "right":               "right",
      "left_input":          "left",
      "right_input":         "right",
      // GroupMap (Structured Streaming)
      "initial_input":       "initial",
      // CoGroupMap
      "other":               "right",
      // WithRelations: root is the active plan, references are sub-plans linked
      // by plan_id. We render references as siblings of root -- it's not a true
      // DAG cross-edge but at least every sub-plan is visible.
      "root":                "root",
      "references":          "ref",
      // MlRelation
      "model_summary_dataset": "summary",
      "dataset":             "ds",  // various ML inner messages
      // Command variants
      "relation":            "rel",                // Checkpoint, Remove*, Pipeline
      "source_table_plan":   "source",             // MergeIntoTableCommand
      "target_table":        "target",             // MergeIntoTableCommand (alt)
    };

    // Operator kinds whose two main inputs benefit from an "L"/"R" edge label.
    var BINARY_OPS = {
      join: 1, lateral_join: 1, as_of_join: 1, nearest_by_join: 1,
      set_op: 1, with_relations: 1, co_group_map: 1,
      merge_into_table_command: 1,
    };

    // ---- Expression formatter (proto Expression -> short string) ----------
    // Walks an Expression block tree and produces a SQL-ish rendering for the
    // common variants used in DataFrame/SQL plans. Anything we don't have a
    // specific formatter for falls back to its proto field name.
    function firstBlockChild(b) {
      var cs = (b && b.children) || [];
      for (var i = 0; i < cs.length; i++) if (cs[i].kind === "block") return cs[i];
      return null;
    }
    function findScalar(b, key) {
      var cs = (b && b.children) || [];
      for (var i = 0; i < cs.length; i++) {
        if (cs[i].kind === "scalar" && cs[i].key === key) return cs[i];
      }
      return null;
    }
    function findBlocks(b, key) {
      var out = [];
      var cs = (b && b.children) || [];
      for (var i = 0; i < cs.length; i++) {
        if (cs[i].kind === "block" && cs[i].key === key) out.push(cs[i]);
      }
      return out;
    }

    // Pretty-print one Expression message. ``wrapper`` may either be a
    // wrapper-field block (e.g. ``condition`` or one entry of an
    // ``aggregate_expressions`` repeated field) or the variant block itself
    // (e.g. ``unresolved_function``). The function descends through wrappers
    // until it finds a known oneof variant.
    function formatExpr(wrapper, depth) {
      depth = depth || 0;
      if (depth > 6) return "…";
      if (!wrapper) return "?";
      if (wrapper.kind === "scalar") {
        return wrapper.vtype === "str" ? '"' + wrapper.value + '"'
          : String(wrapper.value);
      }
      if (wrapper.kind === "trunc") return "…";
      // For wrapper blocks (Expression has only a oneof variant inside), step
      // down once to the chosen variant. For variant blocks (e.g. when called
      // recursively on ``literal`` directly), proceed without stepping.
      var EXPR_VARIANTS = {
        literal: 1, unresolved_attribute: 1, unresolved_function: 1, alias: 1,
        expression_string: 1, cast: 1, sort_order: 1, lambda_function: 1,
        unresolved_named_lambda_variable: 1, window: 1, unresolved_star: 1,
        unresolved_regex: 1, unresolved_extract_value: 1,
        update_fields: 1, common_inline_user_defined_function: 1,
        call_function: 1, typed_aggregate_expression: 1, named_argument_expression: 1,
        merge_action: 1, lazy_expression: 1, subquery_expression: 1,
      };
      var node = wrapper;
      if (!EXPR_VARIANTS.hasOwnProperty(node.key)) {
        var inner = firstBlockChild(node);
        if (inner) node = inner;
      }

      switch (node.key) {
        case "literal": {
          // Literal { <type-tagged field>: value }
          var prim = (node.children || []).find(function (c) { return c.kind === "scalar"; });
          if (prim) {
            return prim.vtype === "str" ? '"' + prim.value + '"' : String(prim.value);
          }
          var pb = firstBlockChild(node);
          // No content: server's ProtoUtils.abbreviate clears messages past the
          // nesting-level cap. Show truncation marker rather than a misleading
          // "null".
          return pb ? pb.key : "…";
        }
        case "unresolved_attribute": {
          var id = findScalar(node, "unparsed_identifier");
          return id ? id.value : "…";  // see literal note above
        }
        case "unresolved_function": {
          var fname = findScalar(node, "function_name");
          var args = findBlocks(node, "arguments");
          var argStr = args.map(function (a) { return formatExpr(a, depth + 1); }).join(", ");
          return (fname ? fname.value : "<fn>") + "(" + argStr + ")";
        }
        case "alias": {
          var name = findScalar(node, "name");
          var expr = findBlocks(node, "expr")[0];
          var rhs = name ? name.value : "<alias>";
          return formatExpr(expr, depth + 1) + " AS " + rhs;
        }
        case "expression_string": {
          var s = findScalar(node, "expression");
          return s ? s.value : "?";
        }
        case "cast": {
          var e = findBlocks(node, "expr")[0];
          var t = findScalar(node, "type_str") || findBlocks(node, "type")[0];
          var tStr = t && t.kind === "scalar" ? t.value : (t ? t.key : "?");
          return "CAST(" + formatExpr(e, depth + 1) + " AS " + tStr + ")";
        }
        case "sort_order": {
          var child = findBlocks(node, "child")[0];
          var dir = findScalar(node, "direction");
          var dirS = dir ? formatEnum(dir.value, "SORT_DIRECTION_") : "";
          return formatExpr(child, depth + 1) + (dirS ? " " + dirS : "");
        }
        case "call_function": {
          var fn = findScalar(node, "function_name");
          var as = findBlocks(node, "arguments");
          return (fn ? fn.value : "<fn>") + "("
            + as.map(function (a) { return formatExpr(a, depth + 1); }).join(", ") + ")";
        }
        case "unresolved_star": {
          var tgt = findScalar(node, "unparsed_target");
          return tgt ? tgt.value : "*";
        }
        case "unresolved_regex": {
          var c = findScalar(node, "col_name");
          return c ? c.value : "<regex>";
        }
      }
      // Unknown variant: return its proto name as a best-effort hint.
      return node.key;
    }

    function summarizeExprList(blocks) {
      if (!blocks || blocks.length === 0) return "";
      var parts = [];
      var limit = Math.min(blocks.length, 3);
      for (var i = 0; i < limit; i++) parts.push(formatExpr(blocks[i]));
      if (blocks.length > limit) parts.push("+" + (blocks.length - limit) + " more");
      return parts.join(", ");
    }

    // Build a DAG node from a Relation block (the wrapper `root` / `command`
    // or an `input` / `left` / `right` field's value).
    function buildDagNode(relBlock) {
      if (!relBlock || !relBlock.children) return null;
      var op = null;
      // Skip `common { ... }`; the first remaining block IS the operator (the
      // `oneof rel_type` chosen for this Relation).
      for (var i = 0; i < relBlock.children.length; i++) {
        var c = relBlock.children[i];
        if (c.kind !== "block") continue;
        if (c.key === "common") continue;
        op = c; break;
      }
      if (!op) return null;

      var children = [];
      var scalars = [];
      var exprs = {};         // grouped by key for short summaries
      var truncated = false;
      for (var j = 0; j < op.children.length; j++) {
        var c2 = op.children[j];
        if (c2.kind === "trunc") { truncated = true; continue; }
        if (c2.kind === "block" && CHILD_FIELDS.hasOwnProperty(c2.key)) {
          var sub = buildDagNode(c2);
          if (sub) {
            sub.edgeLabel = CHILD_FIELDS[c2.key];
            children.push(sub);
          }
        } else if (c2.kind === "scalar") {
          scalars.push(c2);
        } else if (c2.kind === "block") {
          if (!exprs[c2.key]) exprs[c2.key] = [];
          exprs[c2.key].push(c2);
        }
      }

      var sub = [];
      // Show a small, op-specific summary. The full proto text is in the tooltip.
      if (op.key === "range") {
        var s = scalars.find(function (x) { return x.key === "start"; });
        var e = scalars.find(function (x) { return x.key === "end"; });
        var st = scalars.find(function (x) { return x.key === "step"; });
        var rng = (s ? s.value : "0") + " .. " + (e ? e.value : "?");
        if (st && st.value !== "1") rng += " step " + st.value;
        sub.push(rng);
      } else if (op.key === "sql") {
        var q = scalars.find(function (x) { return x.key === "query"; });
        if (q) sub.push(truncate(q.value, 60));
      } else if (op.key === "filter") {
        if (exprs.condition) sub.push("where " + summarizeExprList(exprs.condition));
      } else if (op.key === "project") {
        if (exprs.expressions) sub.push("select " + summarizeExprList(exprs.expressions));
      } else if (op.key === "aggregate") {
        if (exprs.grouping_expressions) {
          sub.push("by " + summarizeExprList(exprs.grouping_expressions));
        }
        if (exprs.aggregate_expressions) {
          sub.push("agg " + summarizeExprList(exprs.aggregate_expressions));
        }
      } else if (op.key === "limit" || op.key === "offset") {
        var lim = scalars.find(function (x) { return x.key === op.key; });
        if (lim) sub.push(op.key + " " + lim.value);
      } else if (op.key === "join" || op.key === "as_of_join" || op.key === "lateral_join"
                 || op.key === "nearest_by_join") {
        var jt = scalars.find(function (x) { return x.key === "join_type"; });
        if (jt) sub.push(formatEnum(jt.value, "JOIN_TYPE_"));
        if (exprs.join_condition) sub.push("on " + summarizeExprList(exprs.join_condition));
      } else if (op.key === "set_op") {
        var st2 = scalars.find(function (x) { return x.key === "set_op_type"; });
        if (st2) sub.push(formatEnum(st2.value, "SET_OP_TYPE_"));
      } else if (op.key === "sort") {
        // Sort.order is repeated SortOrder (not an Expression itself), so we
        // format it directly instead of calling formatExpr on the wrapper.
        if (exprs.order) {
          var ordStrs = exprs.order.slice(0, 3).map(function (o) {
            var ch = findBlocks(o, "child")[0];
            var dir = findScalar(o, "direction");
            return formatExpr(ch) + (dir ? " " + formatEnum(dir.value, "SORT_DIRECTION_") : "");
          });
          if (exprs.order.length > 3) ordStrs.push("+" + (exprs.order.length - 3));
          sub.push("by " + ordStrs.join(", "));
        }
      } else if (op.key === "read") {
        var ds = (op.children || []).find(function (x) {
          return x.kind === "block" && x.key === "data_source";
        });
        if (ds) {
          var fmt = (ds.children || []).find(function (x) { return x.kind === "scalar" && x.key === "format"; });
          if (fmt) sub.push("format " + fmt.value);
        }
      } else if (op.key === "show_string" || op.key === "html_string") {
        var nr = scalars.find(function (x) { return x.key === "num_rows"; });
        var trun = scalars.find(function (x) { return x.key === "truncate"; });
        if (nr) sub.push("num_rows " + nr.value + (trun ? " truncate " + trun.value : ""));
      } else if (op.key === "local_relation") {
        sub.push("inline");
      } else if (op.key === "subquery_alias") {
        var al = scalars.find(function (x) { return x.key === "alias"; });
        if (al) sub.push('as "' + al.value + '"');
      } else if (op.key === "repartition" || op.key === "repartition_by_expression") {
        var np = scalars.find(function (x) { return x.key === "num_partitions"; });
        if (np) sub.push("partitions " + np.value);
      } else if (op.key === "with_columns") {
        if (exprs.aliases) sub.push((exprs.aliases.length) + " column(s)");
      } else if (op.key === "with_columns_renamed") {
        if (exprs.rename_columns_map) sub.push(exprs.rename_columns_map.length + " mapping(s)");
        else if (exprs.renames) sub.push(exprs.renames.length + " rename(s)");
      } else if (op.key === "drop") {
        var names = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "column_names";
        });
        if (names.length) sub.push("drop " + names.map(function (n) { return n.value; }).slice(0, 3).join(", ")
          + (names.length > 3 ? " +" + (names.length - 3) : ""));
        else if (exprs.columns) sub.push(exprs.columns.length + " column(s)");
      } else if (op.key === "tail") {
        var t = scalars.find(function (x) { return x.key === "limit"; });
        if (t) sub.push("tail " + t.value);
      } else if (op.key === "sample") {
        // proto3 defaults (lower_bound=0.0) are elided from text format, so
        // either bound being present is enough to render a useful summary.
        var lo = scalars.find(function (x) { return x.key === "lower_bound"; });
        var hi = scalars.find(function (x) { return x.key === "upper_bound"; });
        var wr = scalars.find(function (x) { return x.key === "with_replacement"; });
        var loV = lo ? lo.value : "0";
        if (hi) sub.push("frac " + loV + "..." + hi.value
          + (wr && wr.value === "true" ? " (replace)" : ""));
        else if (lo) sub.push("frac " + lo.value + "..."
          + (wr && wr.value === "true" ? " (replace)" : ""));
      } else if (op.key === "deduplicate") {
        var cnames = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "column_names";
        });
        if (cnames.length) sub.push("by " + cnames.map(function (n) { return n.value; })
          .slice(0, 3).join(", "));
        else sub.push("all columns");
      } else if (op.key === "to_df") {
        var nms = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "column_names";
        });
        if (nms.length) sub.push(nms.length + " column(s)");
      } else if (op.key === "hint") {
        var hn = scalars.find(function (x) { return x.key === "name"; });
        if (hn) sub.push('"' + hn.value + '"');
      } else if (op.key === "unpivot") {
        if (exprs.ids) sub.push(exprs.ids.length + " id(s)");
        if (exprs.values) sub.push(exprs.values.length + " value(s)");
      } else if (op.key === "transpose") {
        if (exprs.index_columns) sub.push(exprs.index_columns.length + " index col(s)");
      } else if (op.key === "collect_metrics") {
        var mn = scalars.find(function (x) { return x.key === "name"; });
        if (mn) sub.push('"' + mn.value + '"');
        if (exprs.metrics) sub.push(exprs.metrics.length + " metric(s)");
      } else if (op.key === "to_schema") {
        sub.push("set schema");
      } else if (op.key === "with_watermark") {
        var et = scalars.find(function (x) { return x.key === "event_time"; });
        var dt = scalars.find(function (x) { return x.key === "delay_threshold"; });
        if (et) sub.push("on " + et.value + (dt ? " (" + dt.value + ")" : ""));
      } else if (op.key === "parse") {
        var fmt2 = scalars.find(function (x) { return x.key === "format"; });
        if (fmt2) sub.push(formatEnum(fmt2.value, "PARSE_FORMAT_"));
      } else if (op.key === "fill_na" || op.key === "drop_na" || op.key === "replace") {
        var howCols = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "cols";
        });
        if (howCols.length) sub.push(howCols.length + " column(s)");
      } else if (op.key === "summary") {
        var ss = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "statistics";
        });
        if (ss.length) sub.push(ss.length + " statistic(s)");
      } else if (op.key === "describe") {
        var dc = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "cols";
        });
        if (dc.length) sub.push(dc.length + " column(s)");
      } else if (op.key === "crosstab" || op.key === "cov" || op.key === "corr") {
        var c1 = scalars.find(function (x) { return x.key === "col1"; });
        var c2 = scalars.find(function (x) { return x.key === "col2"; });
        if (c1 && c2) sub.push(c1.value + " × " + c2.value);
      } else if (op.key === "approx_quantile") {
        var ac = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && x.key === "cols";
        });
        if (ac.length) sub.push(ac.length + " column(s)");
      } else if (op.key === "freq_items" || op.key === "sample_by") {
        var fc = (op.children || []).filter(function (x) {
          return x.kind === "scalar" && (x.key === "cols" || x.key === "col");
        });
        if (fc.length) sub.push(fc.length + " column(s)");
      } else if (op.key === "ml_relation") {
        // Variant: transform { ... } or fetch { ... }
        var inner2 = firstBlockChild(op);
        if (inner2) sub.push(inner2.key);
      } else if (op.key === "catalog") {
        var inner3 = firstBlockChild(op);
        if (inner3) sub.push(inner3.key);
      } else if (op.key === "cached_local_relation"
                 || op.key === "chunked_cached_local_relation"
                 || op.key === "cached_remote_relation") {
        var hash = scalars.find(function (x) { return x.key === "hash"; });
        var rid = scalars.find(function (x) { return x.key === "relation_id"; });
        if (rid) sub.push("id " + rid.value);
        else if (hash) sub.push("hash " + truncate(hash.value, 12));
      } else if (op.key === "subquery_alias") {
        // already handled above; placeholder to silence lints
      } else if (op.key === "unresolved_table_valued_function") {
        var ufn = scalars.find(function (x) { return x.key === "function_name"; });
        if (ufn) sub.push(ufn.value + "(…)");
      } else if (op.key === "common_inline_user_defined_table_function"
                 || op.key === "common_inline_user_defined_data_source") {
        var udfn = scalars.find(function (x) { return x.key === "function_name"
          || x.key === "name"; });
        if (udfn) sub.push('"' + udfn.value + '"');
      // Commands
      } else if (op.key === "sql_command") {
        var sq = scalars.find(function (x) { return x.key === "sql"; });
        if (sq) sub.push(truncate(sq.value, 60));
      } else if (op.key === "create_dataframe_view") {
        var vn = scalars.find(function (x) { return x.key === "name"; });
        var ig = scalars.find(function (x) { return x.key === "is_global"; });
        sub.push((vn ? '"' + vn.value + '"' : "view")
          + (ig && ig.value === "true" ? " (global)" : ""));
      } else if (op.key === "write_operation") {
        var wm = scalars.find(function (x) { return x.key === "mode"; });
        var wsrc = scalars.find(function (x) { return x.key === "source"; });
        if (wsrc) sub.push(wsrc.value);
        if (wm) sub.push(formatEnum(wm.value, "SAVE_MODE_"));
      } else if (op.key === "write_operation_v2") {
        var wm2 = scalars.find(function (x) { return x.key === "mode"; });
        if (wm2) sub.push(formatEnum(wm2.value, "MODE_"));
      } else if (op.key === "write_stream_operation_start") {
        var wfmt = scalars.find(function (x) { return x.key === "format"; });
        var qn = scalars.find(function (x) { return x.key === "query_name"; });
        if (qn) sub.push('"' + qn.value + '"');
        if (wfmt) sub.push("format " + wfmt.value);
      } else if (op.key === "merge_into_table_command") {
        var tt = scalars.find(function (x) { return x.key === "target_table_name"; });
        if (tt) sub.push("into " + tt.value);
      } else if (op.key === "checkpoint_command") {
        var lz = scalars.find(function (x) { return x.key === "eager"; });
        sub.push(lz && lz.value === "true" ? "eager" : "lazy");
      } else if (op.key === "execute_external_command") {
        var runner = scalars.find(function (x) { return x.key === "runner"; });
        if (runner) sub.push(runner.value);
      } else if (op.key === "ml_command") {
        var mc = firstBlockChild(op);
        if (mc) sub.push(mc.key);
      } else if (op.key === "pipeline_command") {
        var pc = firstBlockChild(op);
        if (pc) sub.push(pc.key);
      } else if (op.key === "register_function" || op.key === "register_table_function"
                 || op.key === "register_data_source") {
        var rn = scalars.find(function (x) { return x.key === "function_name" || x.key === "name"; });
        if (rn) sub.push('"' + rn.value + '"');
      } else if (op.key === "streaming_query_command"
                 || op.key === "streaming_query_manager_command"
                 || op.key === "streaming_query_listener_bus_command") {
        var sc = firstBlockChild(op);
        if (sc) sub.push(sc.key);
      } else if (op.key === "create_resource_profile_command") {
        sub.push("create");
      } else if (op.key === "remove_cached_remote_relation_command") {
        sub.push("remove");
      } else if (op.key === "get_resources_command") {
        sub.push("get resources");
      }
      if (truncated && sub.length === 0) sub.push("(truncated)");

      // Hover tooltip: a flattened scalar dump of the op block. We trim very
      // long values so the title attribute remains usable.
      var tipParts = [];
      for (var k = 0; k < scalars.length; k++) {
        tipParts.push(scalars[k].key + ": " + truncate(String(scalars[k].value), 80));
      }
      for (var kk in exprs) {
        if (!exprs.hasOwnProperty(kk)) continue;
        tipParts.push(kk + " (" + exprs[kk].length + ")");
      }
      var tooltip = (op.key + (tipParts.length ? "\\n" + tipParts.join("\\n") : ""));

      return {
        op: op.key,
        sub: sub,
        children: children,
        tooltip: tooltip,
        binary: BINARY_OPS.hasOwnProperty(op.key),
        leaf: children.length === 0,
      };
    }

    function truncate(s, n) {
      s = String(s);
      return s.length <= n ? s : s.substring(0, n - 1) + "…";
    }
    function formatEnum(v, prefix) {
      // Enum values are either int literals or symbolic names. The proto text
      // emitter uses symbolic names for known enums.
      v = String(v);
      if (v.indexOf(prefix) === 0) v = v.substring(prefix.length);
      return v.toLowerCase().replace(/_/g, " ");
    }

    // ---- DAG layout & SVG rendering ---------------------------------------
    var NODE_W = 170;
    var NODE_H = 50;
    var H_GAP  = 26;
    var V_GAP  = 78;

    function measure(node) {
      if (node.children.length === 0) { node.subWidth = NODE_W; return; }
      var total = 0;
      for (var i = 0; i < node.children.length; i++) {
        measure(node.children[i]);
        total += node.children[i].subWidth;
      }
      total += (node.children.length - 1) * H_GAP;
      node.subWidth = Math.max(total, NODE_W);
    }

    function assign(node, xOffset, depth) {
      node.x = xOffset + node.subWidth / 2;
      node.y = depth * V_GAP;
      if (node.children.length === 0) return;
      var totalChildW = 0;
      for (var i = 0; i < node.children.length; i++) {
        totalChildW += node.children[i].subWidth;
      }
      totalChildW += (node.children.length - 1) * H_GAP;
      var cx = node.x - totalChildW / 2;
      for (var j = 0; j < node.children.length; j++) {
        var c = node.children[j];
        assign(c, cx, depth + 1);
        cx += c.subWidth + H_GAP;
      }
    }

    function maxDepth(node) {
      if (node.children.length === 0) return 0;
      var d = 0;
      for (var i = 0; i < node.children.length; i++) {
        d = Math.max(d, maxDepth(node.children[i]) + 1);
      }
      return d;
    }

    function renderDagSvg(rootNode) {
      if (!rootNode) return '<div class="dag-empty">No plan information available.</div>';
      measure(rootNode);
      assign(rootNode, 20, 0);
      var depth = maxDepth(rootNode);
      var width  = rootNode.subWidth + 40;
      var height = (depth + 1) * V_GAP + NODE_H + 20;

      var nodesHtml = "";
      var edgesHtml = "";
      (function walk(n) {
        for (var i = 0; i < n.children.length; i++) {
          var c = n.children[i];
          // Bezier from parent bottom-center to child top-center.
          var x1 = n.x, y1 = n.y + NODE_H;
          var x2 = c.x, y2 = c.y;
          var dy = (y2 - y1) / 2;
          var d = "M " + x1 + "," + y1
                + " C " + x1 + "," + (y1 + dy) + " "
                       + x2 + "," + (y2 - dy) + " "
                       + x2 + "," + y2;
          edgesHtml += '<path class="edge" d="' + d + '" marker-end="url(#arrow)"></path>';
          if (n.binary && c.edgeLabel) {
            var lx = (x1 + x2) / 2 + (c.edgeLabel === "left" ? -8 : 8);
            var ly = (y1 + y2) / 2;
            edgesHtml += '<text class="nsub" x="' + lx + '" y="' + ly
              + '" text-anchor="middle">' + (c.edgeLabel === "left" ? "L" : "R") + '</text>';
          }
          walk(c);
        }
        var cls = "nbox" + (n.leaf ? " nbox-leaf" : "") + (n.binary ? " nbox-binary" : "");
        var x = n.x - NODE_W / 2;
        var subY = 32;
        var subTexts = "";
        for (var s = 0; s < Math.min(n.sub.length, 2); s++) {
          subTexts += '<text class="nsub" x="' + (NODE_W / 2) + '" y="' + (subY + s * 14)
            + '" text-anchor="middle">' + escapeHtml(truncate(n.sub[s], 26)) + '</text>';
        }
        nodesHtml += '<g transform="translate(' + x + ',' + n.y + ')">'
          + '<title>' + escapeHtml(n.tooltip) + '</title>'
          + '<rect class="' + cls + '" width="' + NODE_W + '" height="' + NODE_H
          + '" rx="6" ry="6"></rect>'
          + '<text class="ntitle" x="' + (NODE_W / 2) + '" y="20" text-anchor="middle">'
          + escapeHtml(n.op) + '</text>'
          + subTexts
          + '</g>';
      })(rootNode);

      return '<div class="dag-wrap"><svg class="dag" width="' + width
        + '" height="' + height + '" xmlns="http://www.w3.org/2000/svg">'
        + '<defs>'
        + '<marker id="arrow" viewBox="0 -3 7 6" refX="6" refY="0" '
        +   'markerWidth="6" markerHeight="6" orient="auto">'
        + '<path class="arrow" d="M0,-3 L6,0 L0,3 Z"></path></marker>'
        + '</defs>'
        + edgesHtml + nodesHtml
        + '</svg></div>';
    }

    function renderPlanDag(planText, executionId) {
      var fields = parseProto(planText);
      var planBlock = findPlanBlock(fields);
      if (!planBlock) {
        return '<div class="dag-empty">No plan information available.</div>';
      }
      var root = buildDagNode(planBlock);
      return renderDagSvg(root);
    }

    // ---- Inline progress (rendered inside the Status cell) ----------------
    // The progress handler's operation_id (Connect RPC UUID4) and the SQL
    // execution's query_id (SQL engine UUIDv7) are different server-side IDs
    // with no direct link. Match them by start time: each live snapshot picks
    // the closest unmatched execution within a 15-second tolerance. Driving
    // the loop from snaps -> execs (rather than execs -> snaps) is important
    // when two queries start a couple seconds apart: the older completed exec
    // would otherwise greedily grab a snap belonging to the newer in-flight
    // one.
    function buildProgressMap(list, execs) {
      // Only consider snapshots that have live progress (not trivially empty).
      var snaps = (list || []).filter(function (p) {
        return p && !(p.done && p.total_tasks === 0 && p.elapsed_seconds < 0.1);
      });
      if (snaps.length === 0 || !execs || execs.length === 0) return {};

      var TOLERANCE_MS = 15000;
      var usedExec = {};  // query_id -> true once matched
      var m = {};         // execution query_id -> snap

      // Sort snaps by start time so earlier operations match first.
      var sortedSnaps = snaps.slice().sort(function (a, b) {
        return (a.started_wall_ms || 0) - (b.started_wall_ms || 0);
      });
      for (var i = 0; i < sortedSnaps.length; i++) {
        var snap = sortedSnaps[i];
        if (!snap.started_wall_ms) continue;
        var bestExec = null, bestDiff = Infinity;
        for (var j = 0; j < execs.length; j++) {
          var e = execs[j];
          if (!e.submission_time_ms || usedExec[e.query_id]) continue;
          var diff = Math.abs(snap.started_wall_ms - e.submission_time_ms);
          if (diff < bestDiff && diff < TOLERANCE_MS) {
            bestDiff = diff; bestExec = e;
          }
        }
        if (bestExec) { usedExec[bestExec.query_id] = true; m[bestExec.query_id] = snap; }
      }
      return m;
    }

    function buildInlineProgress(p) {
      if (!p) return "";
      // Suppress placeholder that has neither tasks nor elapsed time yet.
      if (p.done && p.total_tasks === 0 && p.elapsed_seconds < 0.1) return "";
      var hasStages = p.total_tasks > 0;
      var pct = hasStages
        ? Math.max(0, Math.min(100, p.completed_tasks * 100 / p.total_tasks))
        : 0;
      var fillStyle, fillCls, labelTxt;
      if (p.done) {
        fillStyle = "width:100%;background:#6b7280;";
        fillCls = "";
        labelTxt = hasStages ? pct.toFixed(0) + "%" : "done";
      } else if (hasStages) {
        fillStyle = "width:" + pct.toFixed(1) + "%;";
        fillCls = "";
        labelTxt = pct.toFixed(1) + "%";
      } else {
        fillStyle = "";
        fillCls = " indeterminate";
        labelTxt = "starting…";
      }
      var stats = (hasStages
          ? p.completed_tasks + "/" + p.total_tasks + " tasks"
          : p.inflight_tasks + " in-flight")
        + " \xb7 " + p.elapsed_seconds.toFixed(1) + "s"
        + " \xb7 " + fmtBytes(p.bytes_read);
      var stagesTxt = (p.stages || []).map(function (s) {
        return "s" + s.stage_id + ": " + s.num_completed_tasks + "/" + s.num_tasks
          + (s.done ? "✓" : "");
      }).join("  ");
      return '<div class="prog-wrap">'
        + '<div class="prog-bar">'
        + '<div class="prog-fill' + fillCls + '" style="' + fillStyle + '"></div>'
        + '</div>'
        + '<div class="prog-label">' + escapeHtml(labelTxt) + ' \xb7 ' + stats + '</div>'
        + (stagesTxt ? '<div class="prog-stages">' + escapeHtml(stagesTxt) + '</div>' : '')
        + '</div>';
    }

    // ---- Execution info renderer (metrics DAG + flows) --------------------
    // plan_nodes: [{name, plan_id, parent_plan_id, metrics:[{name,value,type}]}]
    // We render two sections:
    //  1. Physical plan DAG with every MetricValue shown inside each node
    //  2. Observed metrics (flows) table if non-empty

    var MNODE_W = 200;
    var MNODE_PAD = 8;   // vertical padding inside node box
    var MROW_H  = 13;    // height per metric row
    var MNAME_H = 16;    // height of the operator name line
    var MH_GAP  = 20;
    var MV_GAP  = 50;

    function mNodeHeight(n) {
      return MNAME_H + MNODE_PAD + n.metrics.length * MROW_H + MNODE_PAD;
    }

    function buildMetricsDagNodes(planNodes) {
      // Index by plan_id
      var byId = {};
      planNodes.forEach(function (n) {
        byId[n.plan_id] = {
          plan_id: n.plan_id, parent_plan_id: n.parent_plan_id,
          name: n.name, metrics: n.metrics,
          children: [],
        };
      });
      // Wire children
      var roots = [];
      planNodes.forEach(function (n) {
        var node = byId[n.plan_id];
        var parent = byId[n.parent_plan_id];
        if (parent && parent.plan_id !== node.plan_id) {
          parent.children.push(node);
        } else {
          roots.push(node);
        }
      });
      // If multiple roots, wrap in a virtual root (shouldn't happen normally)
      return roots.length === 1 ? roots[0] : (roots[0] || null);
    }

    function mMeasure(node) {
      if (!node.children || node.children.length === 0) {
        node.subWidth = MNODE_W; return;
      }
      var total = 0;
      node.children.forEach(function (c) { mMeasure(c); total += c.subWidth; });
      total += (node.children.length - 1) * MH_GAP;
      node.subWidth = Math.max(total, MNODE_W);
    }

    function mAssign(node, xOff, depth) {
      var h = mNodeHeight(node);
      node.x = xOff + node.subWidth / 2;
      node.y = depth;
      node.h = h;
      if (!node.children || node.children.length === 0) return;
      var totalW = 0;
      node.children.forEach(function (c) { totalW += c.subWidth; });
      totalW += (node.children.length - 1) * MH_GAP;
      var cx = node.x - totalW / 2;
      node.children.forEach(function (c) {
        mAssign(c, cx, depth + h + MV_GAP);
        cx += c.subWidth + MH_GAP;
      });
    }

    function mTotalHeight(node) {
      if (!node.children || node.children.length === 0) return node.y + node.h;
      var d = node.y + node.h;
      node.children.forEach(function (c) { d = Math.max(d, mTotalHeight(c)); });
      return d;
    }

    function renderMetricsDag(planNodes) {
      if (!planNodes || planNodes.length === 0) return "";
      var root = buildMetricsDagNodes(planNodes);
      if (!root) return "";
      mMeasure(root);
      mAssign(root, 20, 0);
      var width  = root.subWidth + 40;
      var height = mTotalHeight(root) + 10;

      var edges = "", nodes = "";
      (function walk(n) {
        (n.children || []).forEach(function (c) {
          var x1 = n.x, y1 = n.y + n.h;
          var x2 = c.x, y2 = c.y;
          var dy = (y2 - y1) / 2;
          var d = "M " + x1 + "," + y1
            + " C " + x1 + "," + (y1 + dy)
            + " " + x2 + "," + (y2 - dy)
            + " " + x2 + "," + y2;
          edges += '<path class="medge" d="' + d + '" marker-end="url(#marrow)"></path>';
          walk(c);
        });
        var boxCls = "mbox" + (n.plan_id === root.plan_id ? " mbox-root" : "");
        var x = n.x - MNODE_W / 2;
        var rows = "";
        n.metrics.forEach(function (m, mi) {
          var vy = MNAME_H + MNODE_PAD + mi * MROW_H + MROW_H / 2 + 4;
          rows += '<text class="mlabel" x="6" y="' + vy + '">' + escapeHtml(m.name) + '</text>'
            + '<text class="mval" x="' + (MNODE_W - 6) + '" y="' + vy
            + '" text-anchor="end">' + escapeHtml(fmtMetricValue(m)) + '</text>';
        });
        nodes += '<g transform="translate(' + x + ',' + n.y + ')">'
          + '<title>' + escapeHtml(n.name) + '</title>'
          + '<rect class="' + boxCls + '" width="' + MNODE_W + '" height="' + n.h
          + '" rx="5" ry="5"></rect>'
          + '<text class="mname" x="' + (MNODE_W / 2) + '" y="' + (MNAME_H - 3)
          + '" text-anchor="middle">' + escapeHtml(truncate(n.name, 30)) + '</text>'
          + (n.metrics.length > 0
              ? '<line x1="0" y1="' + MNAME_H + '" x2="' + MNODE_W + '" y2="' + MNAME_H
                + '" stroke="#e5e7eb" stroke-width="1"></line>'
              : "")
          + rows
          + '</g>';
      })(root);

      return '<div class="dag-wrap"><svg class="mdag" width="' + width
        + '" height="' + height + '" xmlns="http://www.w3.org/2000/svg">'
        + '<defs><marker id="marrow" viewBox="0 -3 7 6" refX="6" refY="0"'
        + ' markerWidth="6" markerHeight="6" orient="auto">'
        + '<path class="marrow" d="M0,-3 L6,0 L0,3 Z"></path></marker></defs>'
        + edges + nodes + '</svg></div>';
    }

    function fmtMetricValue(m) {
      var v = m.value, t = m.type || "";
      // Bytes heuristic: name contains "bytes", "size", "memory", "spill"
      if (/bytes|size|memory|spill/i.test(m.name) && v > 1024)
        return fmtBytes(v) + " (" + t + ")";
      // Large counts: format with comma separators
      if (typeof v === "number" && Math.abs(v) >= 10000)
        return v.toLocaleString() + " (" + t + ")";
      return v + " (" + t + ")";
    }

    function renderFlows(flows) {
      if (!flows || flows.length === 0) return "";
      var html = '<div class="ei-section"><div class="ei-section-title">Observed Metrics</div>'
        + '<table class="flows-table"><thead><tr><th>Name</th><th>Metric</th><th>Value</th></tr></thead><tbody>';
      flows.forEach(function (f) {
        var keys = Object.keys(f.pairs || {});
        if (keys.length === 0) {
          html += '<tr><td>' + escapeHtml(f.name) + '</td><td colspan="2" class="muted">-</td></tr>';
        } else {
          keys.forEach(function (k, i) {
            html += '<tr>'
              + (i === 0 ? '<td rowspan="' + keys.length + '">' + escapeHtml(f.name) + '</td>' : '')
              + '<td class="mono">' + escapeHtml(k) + '</td>'
              + '<td class="mono">' + escapeHtml(String(f.pairs[k])) + '</td>'
              + '</tr>';
          });
        }
      });
      return html + '</tbody></table></div>';
    }

    function renderExecutionInfo(info) {
      if (!info) return "";
      var html = '<div class="ei-wrap">';
      if (info.plan_nodes && info.plan_nodes.length > 0) {
        html += '<div class="ei-section"><div class="ei-section-title">Physical Plan Metrics</div>'
          + renderMetricsDag(info.plan_nodes) + '</div>';
      }
      html += renderFlows(info.flows);
      return html + '</div>';
    }

    // ---- Table rendering ---------------------------------------------------
    function statusClass(s) { return "status-" + s; }

    function renderRows(execs, progressMap) {
      var rows = document.getElementById("rows");
      var emptyEl = document.getElementById("empty");
      if (!execs || execs.length === 0) {
        rows.innerHTML = "";
        emptyEl.style.display = "";
        return;
      }
      emptyEl.style.display = "none";
      execs.sort(function (a, b) { return b.execution_id - a.execution_id; });

      var html = "";
      for (var i = 0; i < execs.length; i++) {
        var e = execs[i];
        var qid = e.query_id || "";
        var qidCell = qid
          ? '<span title="' + escapeHtml(qid) + '">'
              + escapeHtml(qid.substring(0, 8)) + '…</span>'
          : '<span class="muted">-</span>';
        var userCell = e.user_id ? escapeHtml(e.user_id) : '<span class="muted">-</span>';
        var planKey    = "exec::" + e.execution_id + "::plan";
        var errKey     = "exec::" + e.execution_id + "::error";
        var metricsKey = "exec::" + e.execution_id + "::metrics";

        var prog = progressMap[qid] || null;
        var statusHtml = '<span class="' + statusClass(e.status) + '">'
          + escapeHtml(e.status) + '</span>'
          + buildInlineProgress(prog);

        var planText = e.details || e.description || "";
        var execInfo = (prog && prog.execution_info) ? prog.execution_info : null;

        var tagsHtml = '<span class="muted">-</span>';
        if (e.tags && e.tags.length > 0) {
          tagsHtml = e.tags.map(function (t) {
            return '<span class="tag-badge">' + escapeHtml(t) + '</span>';
          }).join("");
        }

        html += '<tr>'
          + '<td class="mono">' + e.execution_id + '</td>'
          + '<td class="mono">' + qidCell + '</td>'
          + '<td>' + userCell + '</td>'
          + '<td>' + tagsHtml + '</td>'
          + '<td>' + statusHtml + '</td>'
          + '<td class="mono">' + fmtTime(e.submission_time_ms) + '</td>'
          + '<td class="mono">' + fmtDuration(e.submission_time_ms, e.completion_time_ms) + '</td>'
          + '</tr>';
        html += '<tr class="detail-row"><td></td><td colspan="6">';
        html += '<details open data-detkey="' + escapeHtml(planKey) + '">'
          + '<summary>Plan</summary>'
          + renderPlanDag(planText, e.execution_id)
          + '</details>';
        if (execInfo) {
          html += '<details data-detkey="' + escapeHtml(metricsKey) + '">'
            + '<summary>Metrics</summary>'
            + renderExecutionInfo(execInfo)
            + '</details>';
        }
        if (e.error_message) {
          html += '<details open data-detkey="' + escapeHtml(errKey) + '">'
            + '<summary>Error</summary>'
            + '<pre class="error">' + escapeHtml(e.error_message) + '</pre>'
            + '</details>';
        }
        html += '</td></tr>';
      }
      rows.innerHTML = html;
      applyOpenState(rows);
    }

    // ---- Polling loop ------------------------------------------------------
    var inFlight = false;

    function refresh() {
      if (inFlight) return;
      inFlight = true;
      fetch("/api/sql.json", { headers: { "Accept": "application/json" } })
        .then(function (r) {
          if (!r.ok) throw new Error("HTTP " + r.status);
          return r.json();
        })
        .then(function (data) {
          document.getElementById("error").style.display = "none";
          document.getElementById("fetched-at").textContent = data.fetched_at;
          document.getElementById("row-count").textContent =
            (data.executions || []).length;
          if (data.user_id)    document.getElementById("hdr-user").textContent    = data.user_id;
          if (data.session_id) document.getElementById("hdr-session").textContent =
            data.session_id.substring(0, 8) + "…";
          var execs = data.executions || [];
          renderRows(execs, buildProgressMap(data.progress_list, execs));
        })
        .catch(function (err) {
          var e = document.getElementById("error");
          e.textContent = "Failed to refresh: " + err;
          e.style.display = "block";
        })
        .then(function () { inFlight = false; });
    }

    refresh();
    setInterval(refresh, POLL_MS);
  })();
  </script>
</body>
</html>
"""


def _summary_to_dict(s: "SqlExecutionSummary") -> Dict[str, Any]:
    return dataclasses.asdict(s)


def _progress_snap_to_dict(snap: Any) -> Dict[str, Any]:
    return {
        "operation_id": snap.operation_id,
        "total_tasks": snap.total_tasks,
        "completed_tasks": snap.completed_tasks,
        "inflight_tasks": snap.inflight_tasks,
        "bytes_read": snap.bytes_read,
        "elapsed_seconds": round(snap.elapsed_seconds, 2),
        "done": snap.done,
        "started_wall_ms": snap.started_wall_ms,
        "stages": [dataclasses.asdict(st) for st in snap.stages],
        "execution_info": snap.execution_info,
    }


def _progress_list_to_dict(handler: "_UIProgressHandler") -> List[Dict[str, Any]]:
    return [_progress_snap_to_dict(s) for s in handler.snapshots()]


def make_app(
    spark: "SparkSession",
    refresh_seconds: int = 5,
    remote: Optional[str] = None,
    progress_handler: Optional["_UIProgressHandler"] = None,
) -> Flask:
    display_remote = remote or os.environ.get("SPARK_REMOTE") or "<connect server>"
    user_id = getattr(spark.client, "_user_id", None) or ""
    session_id = getattr(spark.client, "_session_id", None) or ""
    app = Flask(__name__)

    @app.route("/")
    def index() -> str:
        return render_template_string(
            _SHELL,
            remote=display_remote,
            poll_ms=_API_POLL_MS,
        )

    @app.route("/sql")
    def sql() -> str:
        return index()

    @app.route("/api/sql.json")
    def api_sql() -> Any:
        execs: List["SqlExecutionSummary"] = list_sql_executions(
            spark, offset=0, length=200
        )
        return jsonify(
            {
                "fetched_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "refresh_seconds": refresh_seconds,
                "remote": display_remote,
                "user_id": user_id,
                "session_id": session_id,
                "progress_list": (
                    _progress_list_to_dict(progress_handler)
                    if progress_handler is not None else []
                ),
                "executions": [_summary_to_dict(s) for s in execs],
            }
        )

    return app
