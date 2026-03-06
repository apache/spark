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

/* global $, uiRoot */

import { getStandAloneAppId, setDataTableDefaults } from "./utils.js";

function createRESTEndPointForEnvironmentPage(appId) {
  var words = document.baseURI.split("/");
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join("/");
    return newBaseURI + "/api/v1/applications/" + appId + "/environment";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join("/");
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId + "/environment";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/environment";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId + "/environment";
}

function updateBadge(tabId, count) {
  var tab = document.getElementById(tabId);
  if (tab) {
    var existing = tab.querySelector(".badge");
    if (existing) {
      existing.textContent = count;
    } else {
      var badge = document.createElement("span");
      badge.className = "badge bg-secondary ms-1";
      badge.textContent = count;
      tab.appendChild(badge);
    }
  }
}

function formatResourceProfile(rp) {
  var lines = ["Executor Reqs:"];
  var execRes = rp.executorResources || {};
  Object.keys(execRes).sort().forEach(function (key) {
    var r = execRes[key];
    var s = "\t" + r.resourceName + ": [amount: " + r.amount;
    if (r.discoveryScript) { s += ", discovery: " + r.discoveryScript; }
    if (r.vendor) { s += ", vendor: " + r.vendor; }
    s += "]";
    lines.push(s);
  });
  lines.push("Task Reqs:");
  var taskRes = rp.taskResources || {};
  Object.keys(taskRes).sort().forEach(function (key) {
    var r = taskRes[key];
    lines.push("\t" + r.resourceName + ": [amount: " + r.amount + "]");
  });
  return lines.join("\n");
}

function initDataTable(paneId, tableId, data, columns, dtOpts) {
  var pane = document.getElementById(paneId);
  pane.innerHTML = '<table id="' + tableId +
    '" class="table table-striped compact cell-border" style="width:100%"></table>';
  var opts = $.extend({
    data: data,
    columns: columns,
    order: [[0, "asc"]],
    pageLength: 50,
    deferRender: true,
    language: { search: "Search:&#160;" }
  }, dtOpts || {});
  $("#" + tableId).DataTable(opts);
}

$(document).ready(function () {
  setDataTableDefaults();

  // Tab state persistence
  var storedTab = localStorage.getItem("env-active-tab");
  if (storedTab) {
    var el = document.getElementById(storedTab);
    if (el) {
      bootstrap.Tab.getOrCreateInstance(el).show();
    }
  }

  document.querySelectorAll('#envTabs button[data-bs-toggle="pill"]')
    .forEach(function (tabEl) {
      tabEl.addEventListener("shown.bs.tab", function (event) {
        localStorage.setItem("env-active-tab", event.target.id);
        // Adjust DataTable columns for newly visible tab
        $(event.target.getAttribute("data-bs-target"))
          .find(".dataTable").each(function () {
            $(this).DataTable().columns.adjust();
          });
      });
    });

  getStandAloneAppId(function (appId) {
    var endPoint = createRESTEndPointForEnvironmentPage(appId);
    $.getJSON(endPoint, function (response) {
      // Runtime Information
      var runtime = response.runtime || {};
      var runtimeData = [
        ["Java Version", runtime.javaVersion || ""],
        ["Java Home", runtime.javaHome || ""],
        ["Scala Version", runtime.scalaVersion || ""]
      ];
      initDataTable("runtime", "runtime-table", runtimeData, [
        { title: "Name", width: "35%" },
        { title: "Value", width: "65%" }
      ], { paging: false, searching: false, info: false });
      updateBadge("runtime-tab", runtimeData.length);

      // Spark Properties — highlight non-default values
      var defaultsEl = document.getElementById("spark-config-defaults");
      var configDefaults = {};
      if (defaultsEl) {
        try { configDefaults = JSON.parse(defaultsEl.textContent); } catch (e) { /* ignore */ }
      }

      var sparkProps = response.sparkProperties || [];
      var sparkPropsWithDefaults = sparkProps.map(function (prop) {
        var d = Object.prototype.hasOwnProperty.call(configDefaults, prop[0]) ? configDefaults[prop[0]] : "";
        return [prop[0], prop[1], d];
      });

      initDataTable("spark-props", "spark-props-table", sparkPropsWithDefaults, [
        { title: "Name", width: "30%" },
        { title: "Value", width: "35%" },
        { title: "Default Value", width: "35%" }
      ], {
        createdRow: function (row, data) {
          if (Object.prototype.hasOwnProperty.call(configDefaults, data[0]) && data[1] !== configDefaults[data[0]]) {
            $(row).addClass("table-warning");
          }
        }
      });

      updateBadge("spark-props-tab", sparkProps.length);

      // Resource Profiles
      var profiles = response.resourceProfiles || [];
      var rpData = profiles.map(function (rp) {
        return [String(rp.id), formatResourceProfile(rp)];
      });
      initDataTable("resource-profiles", "resource-profiles-table", rpData, [
        { title: "Resource Profile Id", width: "20%" },
        {
          title: "Resource Profile Contents",
          width: "80%",
          render: function (data) { return "<pre>" + data + "</pre>"; }
        }
      ], { paging: false, searching: false, info: false });
      updateBadge("resource-profiles-tab", rpData.length);

      // Hadoop Properties
      var hadoopProps = response.hadoopProperties || [];
      initDataTable("hadoop-props", "hadoop-props-table", hadoopProps, [
        { title: "Name", width: "35%" },
        { title: "Value", width: "65%" }
      ]);
      updateBadge("hadoop-props-tab", hadoopProps.length);

      // System Properties
      var systemProps = response.systemProperties || [];
      initDataTable("system-props", "system-props-table", systemProps, [
        { title: "Name", width: "35%" },
        { title: "Value", width: "65%" }
      ]);
      updateBadge("system-props-tab", systemProps.length);

      // Metrics Properties
      var metricsProps = response.metricsProperties || [];
      initDataTable("metrics-props", "metrics-props-table", metricsProps, [
        { title: "Name", width: "35%" },
        { title: "Value", width: "65%" }
      ]);
      updateBadge("metrics-props-tab", metricsProps.length);

      // Classpath Entries
      var classpathEntries = response.classpathEntries || [];
      initDataTable("classpath", "classpath-table", classpathEntries, [
        { title: "Resource", width: "35%" },
        { title: "Source", width: "65%" }
      ]);
      updateBadge("classpath-tab", classpathEntries.length);
    });
  });
});
